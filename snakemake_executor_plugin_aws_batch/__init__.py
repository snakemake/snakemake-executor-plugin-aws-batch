__author__ = "Jake VanCampen, Johannes Köster"
__copyright__ = "Copyright 2025, Snakemake community"
__email__ = "jake.vancampen7@gmail.com"
__license__ = "MIT"

from dataclasses import dataclass, field
from pprint import pformat
from typing import List, AsyncGenerator, Optional
from snakemake_executor_plugin_aws_batch.batch_client import BatchClient
from snakemake_executor_plugin_aws_batch.batch_job_builder import BatchJobBuilder
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError


# Optional:
# Define additional settings for your executor.
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
# Make sure that all defined fields are Optional and specify a default value
# of None or anything else that makes sense in your case.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    region: Optional[int] = field(
        default=None,
        metadata={
            "help": "AWS Region",
            "env_var": False,
            "required": True,
        },
    )
    job_queue: Optional[str] = field(
        default=None,
        metadata={
            "help": "The AWS Batch task queue ARN used for running tasks",
            "env_var": True,
            "required": True,
        },
    )
    job_role: Optional[str] = field(
        default=None,
        metadata={
            "help": "The AWS job role ARN that is used for running the tasks",
            "env_var": True,
            "required": True,
        },
    )
    tags: Optional[dict] = field(
        default=None,
        metadata={
            "help": (
                "The tags that should be applied to all of the batch tasks,"
                "of the form KEY=VALUE"
            ),
            "env_var": False,
            "required": False,
        },
    )
    task_timeout: Optional[int] = field(
        default=300,
        metadata={
            "help": (
                "Task timeout (seconds) will force AWS Batch to terminate "
                "a Batch task if it fails to finish within the timeout, minimum 60"
            )
        },
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # whether the executor implies to not have a shared file system
    implies_no_shared_fs=True,
    # whether to deploy workflow sources to default storage provider before execution
    job_deploy_sources=True,
    # whether arguments for setting the storage provider shall be passed to jobs
    pass_default_storage_provider_args=True,
    # whether arguments for setting default resources shall be passed to jobs
    pass_default_resources_args=True,
    # whether environment variables shall be passed to jobs (if False, use
    # self.envvars() to obtain a dict of environment variables and their values
    # and pass them e.g. as secrets to the execution backend)
    pass_envvar_declarations_to_cmd=True,
    # whether the default storage provider shall be deployed before the job is run on
    # the remote node. Usually set to True if the executor does not assume a shared fs
    auto_deploy_default_storage_provider=True,
    # specify initial amount of seconds to sleep before checking for job status
    init_seconds_before_status_checks=0,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        # snakemake/snakemake:latest container image
        self.container_image = self.workflow.remote_execution_settings.container_image

        self.next_seconds_between_status_checks = 5

        self.settings = self.workflow.executor_settings
        self.logger.debug(f"ExecutorSettings: {pformat(self.settings, indent=2)}")

        try:
            self.batch_client = BatchClient(region_name=self.settings.region)
        except Exception as e:
            raise WorkflowError(f"Failed to initialize AWS Batch client: {e}") from e

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.
        # If required, make sure to pass the job's id to the job_info object, as keyword
        # argument 'external_job_id'.

        remote_command = f"/bin/bash -c {self.format_job_exec(job)}"

        try:
            job_definition = BatchJobBuilder(
                logger=self.logger,
                job=job,
                container_image=self.container_image,
                settings=self.settings,
                job_command=remote_command,
                batch_client=self.batch_client,
            )
            job_info = job_definition.submit()
            log_info = {
                "job_name:": job_info["jobName"],
                "jobId": job_info["jobId"],
                "job_queue": self.settings.job_queue,
            }
            self.logger.debug(f"AWS Batch job submitted: {log_info}")
        except Exception as e:
            raise WorkflowError(f"Failed to submit AWS Batch job: {e}") from e

        self.report_job_submission(
            SubmittedJobInfo(
                job=job, external_jobid=job_info["jobId"], aux=dict(job_info)
            )
        )

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None]:
        # Check the status of active jobs.

        # You have to iterate over the given list active_jobs.
        # If you provided it above, each will have its external_jobid set according
        # to the information you provided at submission time.
        # For jobs that have finished successfully, you have to call
        # self.report_job_success(active_job).
        # For jobs that have errored, you have to call
        # self.report_job_error(active_job).
        # This will also take care of providing a proper error message.
        # Usually there is no need to perform additional logging here.
        # Jobs that are still running have to be yielded.
        #
        # For queries to the remote middleware, please use
        # self.status_rate_limiter like this:
        #
        # async with self.status_rate_limiter:
        #    # query remote middleware here
        #
        # To modify the time until the next call of this method,
        # you can set self.next_sleep_seconds here.
        self.logger.debug(f"Monitoring {len(active_jobs)} active Batch jobs")
        for job in active_jobs:
            async with self.status_rate_limiter:
                status_code, msg = self._get_job_status(job)
                if status_code == 0:
                    self.report_job_success(job)
                elif status_code is not None:
                    message = f"AWS Batch job failed. Code: {status_code}, Msg: {msg}."
                    self.report_job_error(job, msg=message)
                else:
                    yield job

    def _get_job_status(self, job: SubmittedJobInfo) -> tuple[int, Optional[str]]:
        """
        Poll for Batch job status and return exit code and message if job is complete.

        Returns:
            tuple: (exit_code, failure_message)
        """
        try:
            response = self.batch_client.describe_jobs(jobs=[job.external_jobid])
            jobs = response.get("jobs", [])

            if not jobs:
                return None, f"No job found with ID {job.external_jobid}"

            job_info: dict = jobs[0]
            job_status = job_info.get("status", "UNKNOWN")

            # push the job_definition_arn to the aux dict for use in cleanup
            job.aux["job_definition_arn"] = job_info.get("jobDefinition", None)
            exit_code = job_info.get("container", {}).get("exitCode", None)

            if job_status == "SUCCEEDED":
                return 0, None
            elif job_status == "FAILED":
                reason = job_info.get("statusReason", "Unknown reason")
                return exit_code or 1, reason
            else:
                log_info = {
                    "job_name": job_info.get("jobName", "unknown"),
                    "job_id": job.external_jobid,
                    "status": job_status,
                }
                self.logger.debug(log_info)
                return None, None
        except Exception as e:
            self.logger.error(f"Error getting job status: {e}")
            return None, str(e)

    def _terminate_job(self, job: SubmittedJobInfo):
        """terminate job from submitted job info"""
        try:
            self.logger.debug(f"terminating job {job.external_jobid}")
            self.batch_client.terminate_job(
                jobId=job.external_jobid,
                reason="terminated by snakemake",
            )
        except Exception as e:
            job_spec = job.aux["job_params"]
            self.logger.info(
                f"failed to terminate Batch job definition: {job_spec} with error: {e}"
            )

    def _deregister_job(self, job: SubmittedJobInfo):
        """deregister batch job definition"""
        try:
            job_def_arn = job.aux.get("job_definition_arn")
            if job_def_arn is not None:
                self.logger.debug(f"de-registering Batch job definition {job_def_arn}")
                self.batch_client.deregister_job_definition(jobDefinition=job_def_arn)
        except Exception as e:
            # AWS expires job definitions after 6mo
            # so failing to delete them isn't fatal
            self.logger.info(
                "failed to deregister Batch job definition "
                f"{job_def_arn} with error {e}"
            )

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        # perform additional steps on shutdown if necessary
        # deregister everything from AWS so the environment is clean
        self.logger.info("shutting down...")
        # cleanup jobs
        for j in active_jobs:
            self._terminate_job(j)
            self._deregister_job(j)
