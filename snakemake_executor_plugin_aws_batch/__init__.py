from dataclasses import dataclass, field
from pprint import pformat
import boto3
import uuid
import botocore
from typing import List, Generator, Optional
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
    fsap_id: Optional[str] = (
        field(
            default=None,
            metadata={
                "help": (
                    "The fsap id of the EFS instance you want to use that "
                    "is shared with your local environment"
                ),
                "env_var": False,
                "required": False,
            },
        ),
    )
    efs_project_path: Optional[str] = (
        field(
            default=None,
            metadata={
                "help": "The EFS path that contains the project Snakemake is running",
                "env_var": False,
                "required": False,
            },
        ),
    )
    task_queue: Optional[str] = field(
        default=None,
        metadata={
            "help": "The AWS Batch task queue ARN used for running tasks",
            "env_var": False,
            "required": True,
        },
    )
    workflow_role: Optional[str] = field(
        default=None,
        metadata={
            "help": "The AWS role that is used for running the tasks",
            "env_var": False,
            "required": True,
        },
    )
    tags: Optional[List[str]] = field(
        default=[],
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
        default=None,
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
    implies_no_shared_fs=True,
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
        # set snakemake/snakemake container image
        self.container_image = self.workflow.remote_execution_settings.container_image

        # access executor specific settings
        self.settings: ExecutorSettings = self.workflow.executor_settings
        self.logger.debug(f"ExecutorSettings: {pformat(self.settings, indent=2)}")

        # keep track of job definitions
        self.created_job_defs = list()

        # init batch client
        try:
            self.aws = boto3.Session().client(  # Session() needed for thread safety
                "batch",
                region_name=self.settings.region,
                config=botocore.config.Config(
                    retries={"max_attempts": 5, "mode": "standard"}
                ),
            )
        except botocore.exceptions.ClientError as e:
            raise WorkflowError(e)

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

        # set job name
        job_uuid = str(uuid.uuid4())
        job_name = f"snakejob-{job.name}-{job_uuid}"

        # set job definition name
        job_definition_name = f"snakejob-def-{job.name}-{job_uuid}"
        job_definition_type = "container"

        # get job resources or default
        vcpu = job.resources.get("_cores", str(1))
        mem = job.resources.get("mem_mb", str(1024))

        # job definition container properties
        container_properties = {
            "executionRoleArn": self.settings.workflow_role,
            "command": ["snakemake"],
            "image": self.container_image,
            "privileged": True,
            "resourceRequirements": [
                {"type": "VCPU", "value": "1"},
                {"type": "MEMORY", "value": "1024"},
            ],
        }

        (
            container_properties["volumes"],
            container_properties["mountPoints"],
        ) = self._prepare_mounts()

        # register the job definition
        try:
            job_def = self.aws.register_job_definition(
                jobDefinitionName=job_definition_name,
                type=job_definition_type,
                containerProperties=container_properties,
                tags=self.settings.tags,
            )
            self.created_job_defs.append(self.job_def)
        except Exception as e:
            raise WorkflowError(e)

        job_command = self._generate_snakemake_command(job)

        # configure job parameters
        job_params = {
            "jobName": job_name,
            "jobQueue": self.settings.task_queue,
            "jobDefinition": "{}:{}".format(
                job_def["jobDefinitionName"], job_def["revision"]
            ),
            "containerOverrides": {
                "command": job_command,
                "resourceRequirements": [
                    {"type": "VCPU", "value": vcpu},
                    {"type": "MEMORY", "value": mem},
                ],
            },
            "tags": self.settings.tags,
        }

        if self.settings.task_timeout is not None:
            job_params["timeout"] = {"attemptDurationSeconds": self.task_timeout}

        # submit the job
        try:
            submitted = self.aws.submit_job(**job_params)
            self.logger.debug(
                "AWS Batch job submitted with queue {}, jobId {} and tags {}".format(
                    self.task_queue, job["jobId"], self.tags
                )
            )
        except Exception as e:
            raise WorkflowError(e)

        self.report_job_submission(
            SubmittedJobInfo(
                job=job,
                external_jobid=submitted["jobId"],
                aux={
                    "jobs_params": job_params,
                    "job_def_arn": job_def["jobDefinitionArn"],
                },
            )
        )

    def _generate_snakemake_command(self, job: JobExecutorInterface) -> str:
        """generates the snakemake command for the job"""
        exec_job = self.format_job_exec(job)
        command = list(filter(None, exec_job.replace('"', "").split(" ")))
        return_command = ["sh", "-c"]
        snakemake_run_command = "cd {}/{} && {}".format(
            self.mount_path, self.efs_project_path, " ".join(command)
        )
        return_command.append(snakemake_run_command)
        return return_command

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
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
                status_code = self._get_job_status(job)
                if status_code == 0:
                    self.report_job_success(job)
                elif status_code is not None:
                    self.report_job_error(job)
                else:
                    yield job

    def _get_job_status(self, job: SubmittedJobInfo) -> (int, Optional[str]):
        """poll for Batch job success or failure

        returns exits code and failure information if exit code is not 0
        """
        known_status_list = [
            "SUBMITTED",
            "PENDING",
            "RUNNABLE",
            "STARTING",
            "RUNNING",
            "SUCCEEDED",
            "FAILED",
        ]
        exit_code = None
        log_stream_name = None
        job_desc = self._describer.describe(self.aws, job.external_jobid, 1)
        job_status = job_desc["status"]

        # set log stream name if not none
        log_details = {"status": job_status, "jobId": job.external_jobid}

        if "container" in job_desc and "logStreamName" in job_desc["container"]:
            log_stream_name = job_desc["container"]["logStreamName"]

        if log_stream_name:
            log_details["logStreamName"] = log_stream_name

        if job_status not in known_status_list:
            self.logger.info(f"unknown job status {job_status} from AWS Batch")
            self.logger.debug("log details: {log_details} with status: {job_status}")

        failure_info = None
        if job_status == "SUCCEEDED":
            return 0, failure_info

        elif job_status == "FAILED":
            reason = job_desc.get("container", {}).get("reason", None)
            status_reason = job_desc.get("statusReason", None)
            failure_info = {"jobId": job.external_jobid}

            if reason:
                failure_info["reason"] = reason

            if status_reason:
                failure_info["statusReason"] = status_reason

            if log_stream_name:
                failure_info["logStreamName"] = log_stream_name

            if (
                status_reason
                and "Host EC2" in status_reason
                and "terminated" in status_reason
            ):
                raise WorkflowError(
                    "AWS Batch job interrupted (likely spot instance termination)"
                    f"with error {failure_info}"
                )

            if "exitCode" not in job_desc.get("container", {}):
                raise WorkflowError(
                    f"AWS Batch job failed with error {failure_info['statusReason']}. "
                    f"View log stream {failure_info['logStreamName']}",
                )

            exit_code = job_desc["container"]["exitCode"]
            assert isinstance(exit_code, int) and exit_code != 0

        return exit_code, failure_info

    def _terminate_job(self, job: SubmittedJobInfo):
        """terminate job from submitted job info"""
        try:
            self.logger.debug(f"terminating job {job.external_jobid}")
            self.aws.terminate_job(
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
            job_def_arn = job.aux["jobDefArn"]
            self.logger.debug(f"de-registering Batch job definition {job_def_arn}")
            self.aws.deregister_job_definition(jobDefinition=job_def_arn)
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
