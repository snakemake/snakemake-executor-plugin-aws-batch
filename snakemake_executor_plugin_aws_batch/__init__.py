__author__ = "Jake VanCampen, Johannes Köster"
__copyright__ = "Copyright 2025, Snakemake community"
__email__ = "jake.vancampen7@gmail.com"
__license__ = "MIT"

from dataclasses import dataclass, field
from pprint import pformat
from typing import List, AsyncGenerator, Optional

import boto3
from botocore.exceptions import ClientError

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


# Batch job-queue / compute-environment `status` values that definitively
# prevent jobs from running. CREATING/UPDATING are transient and must NOT fail
# the preflight check (a queue mid-update is recoverable), so only these abort.
FATAL_BATCH_STATUSES = frozenset({"INVALID", "DELETING", "DELETED"})


# Optional:
# Define additional settings for your executor.
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
# Make sure that all defined fields are Optional and specify a default value
# of None or anything else that makes sense in your case.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    region: Optional[str] = field(
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
        default=None,
        metadata={
            "help": (
                "Task timeout in seconds: AWS Batch terminates a job that does not "
                "finish within this limit. Jobs have no timeout unless this is set. "
                "When set, the value must be at least 60 (the AWS minimum)."
            ),
            "env_var": False,
            "required": False,
        },
    )
    scheduling_priority: Optional[int] = field(
        default=None,
        metadata={
            "help": (
                "Default scheduling priority applied to every submitted job "
                "(schedulingPriorityOverride). Only meaningful on fair-share job "
                "queues, i.e. queues with a scheduling policy attached; ignored "
                "otherwise. Per-rule overrides via the "
                "aws_batch_scheduling_priority resource take precedence."
            ),
            "env_var": False,
            "required": False,
        },
    )
    job_definition: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "Use a pre-existing AWS Batch job definition instead of registering "
                "one per job. Accepts a definition name (e.g. my-def), a name:revision "
                "pair (my-def:3), or a full ARN. When set, resource configuration "
                "(container image, job role, shared memory) is managed externally — "
                "the container_image setting is ignored. Per-job specifics (command, "
                "environment variables, vcpu/mem/gpu) are passed via "
                "containerOverrides. Cannot be combined with --aws-batch-job-role "
                "or the per-rule shared_memory_size_mb resource."
            ),
            "env_var": False,
            "required": False,
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
    pass_envvar_declarations_to_cmd=False,
    # whether the default storage provider shall be deployed before the job is run on
    # the remote node. Set to False so workers do NOT `pip install
    # snakemake-storage-plugin-s3` at startup: that pulls unpinned versions whose
    # newer PyPI metadata requires snakemake-interface-storage-plugins >= 4 and
    # breaks snakemake 8.x. Users must pre-install a compatible plugin version
    # in the container image.
    auto_deploy_default_storage_provider=False,
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

        # Fail fast on a definitively misconfigured queue/compute environment/role
        # before submitting any jobs (degrades to a no-op if state is uncertain).
        self._preflight_validate()

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

        try:
            # Use rule-level container image if specified via resources,
            # otherwise fall back to global container image
            container_image = job.resources.get(
                "aws_batch_container_image", self.container_image
            )

            job_definition = BatchJobBuilder(
                logger=self.logger,
                job=job,
                envvars=self.envvars(),
                container_image=container_image,
                settings=self.settings,
                job_command=self.format_job_exec(job),
                batch_client=self.batch_client,
            )
            job_info = job_definition.submit()
            log_info = {
                "job_name": job_info["jobName"],
                "jobId": job_info["jobId"],
                "job_queue": job_definition.job_queue,
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

            if status_code is not None:
                if status_code == 0:
                    self.report_job_success(job)
                else:
                    message = f"AWS Batch job failed. Code: {status_code}, Msg: {msg}."
                    self.report_job_error(job, msg=message)
                self.cleanup_job_resources(job)
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
                self.logger.debug(str(log_info))
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
            self.logger.info(
                f"failed to terminate Batch job: {job.external_jobid} with error: {e}"
            )

    def _deregister_job(self, job: SubmittedJobInfo):
        """deregister batch job definition"""
        try:
            # Pre-existing job definitions are not owned by this executor;
            # deregistering them would break other workflows using the same definition.
            if job.aux.get("_preexisting_job_definition"):
                self.logger.debug(
                    "skipping deregistration of pre-existing Batch job definition "
                    f"{job.aux.get('job_definition_arn')} "
                    "(externally managed, not owned by this executor)"
                )
                return
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

    def cleanup_job_resources(self, job: SubmittedJobInfo):
        """Terminate and deregister job resources"""
        self._terminate_job(job)
        self._deregister_job(job)

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        # perform additional steps on shutdown if necessary
        # deregister everything from AWS so the environment is clean
        self.logger.info("shutting down...")
        # cleanup jobs
        for j in active_jobs:
            self.cleanup_job_resources(j)

    def _preflight_validate(self) -> None:
        """Fail fast on a definitively misconfigured queue / compute env / role.

        Best-effort about *uncertainty*: a transient API error or a missing
        describe/iam permission degrades to a warning and the workflow proceeds.
        Only a confirmed-bad configuration (disabled/invalid queue or compute
        environment, ``maxvCpus=0``, or a non-existent job role) raises, before
        any job is submitted.
        """
        problems = self._queue_problems()
        if problems:
            raise WorkflowError(
                "AWS Batch preflight check failed — jobs would never start: "
                + "; ".join(problems)
                + ". Check the configured --aws-batch-job-queue and its compute "
                "environment(s)."
            )
        self._validate_job_role()

    def _queue_problems(self) -> Optional[List[str]]:
        """Return definitive job-queue / compute-environment misconfigurations.

        Returns an empty list when everything looks healthy, a non-empty list of
        problem descriptions when the queue or a compute environment is in a
        state that would prevent jobs from ever starting (disabled/invalid,
        ``maxvCpus=0``), or None when the state can't be determined (no queue
        configured, or the queue describe itself failed — never block the
        workflow on a transient failure or a missing describe permission).

        The queue and compute-environment lookups fail independently: a failure
        describing the compute environment(s) (e.g. a missing
        ``batch:DescribeComputeEnvironments`` permission) still returns any
        confirmed queue-level problems collected so far rather than discarding
        them, so a definitively disabled/invalid queue is never masked by an
        unrelated failure downstream.

        Compute environments are judged collectively: AWS Batch falls back
        across a queue's ``computeEnvironmentOrder``, so a compute-environment
        problem is reported only when *every* attached environment is unusable —
        one healthy environment is enough for jobs to run.
        """
        queue_arn = getattr(self.settings, "job_queue", None)
        if not queue_arn:
            return None
        # If we can't even describe the queue, the state is unknown — degrade to
        # a no-op rather than blocking the workflow on a transient failure or a
        # missing batch:DescribeJobQueues permission.
        try:
            queues = self.batch_client.describe_job_queues(jobQueues=[queue_arn]).get(
                "jobQueues", []
            )
        except Exception as e:
            self.logger.debug(f"could not check job queue: {e}")
            return None
        if not queues:
            return ["job queue not found"]
        jq = queues[0]
        problems: List[str] = []
        if jq.get("state") != "ENABLED":
            problems.append(f"job queue is {jq.get('state')} (not ENABLED)")
        if jq.get("status") in FATAL_BATCH_STATUSES:
            problems.append(f"job queue status is {jq.get('status')}")
        ce_arns = [
            o.get("computeEnvironment") for o in jq.get("computeEnvironmentOrder", [])
        ]
        ce_arns = [c for c in ce_arns if c]
        if ce_arns:
            # A failure here must NOT discard the confirmed queue-level problems
            # already collected above — return them instead of None.
            try:
                ces = self.batch_client.describe_compute_environments(
                    computeEnvironments=ce_arns
                ).get("computeEnvironments", [])
            except Exception as e:
                self.logger.debug(f"could not check compute environment(s): {e}")
                return problems
            # AWS Batch tries a queue's compute environments in order and falls
            # back to the next, so jobs are only definitively blocked when EVERY
            # attached compute environment is unusable. Collect per-CE reasons
            # but surface them only when none is usable — one healthy compute
            # environment is enough for jobs to run.
            ce_reasons: List[str] = []
            any_usable = False
            for ce in ces:
                name = ce.get("computeEnvironmentName", "?")
                reasons: List[str] = []
                if ce.get("state") != "ENABLED":
                    reasons.append(f"compute environment {name} is {ce.get('state')}")
                if ce.get("status") in FATAL_BATCH_STATUSES:
                    reasons.append(
                        f"compute environment {name} status is {ce.get('status')}"
                    )
                if (ce.get("computeResources") or {}).get("maxvCpus") == 0:
                    reasons.append(f"compute environment {name} has maxvCpus=0")
                if reasons:
                    ce_reasons.extend(reasons)
                else:
                    any_usable = True
            if ces and not any_usable:
                problems.append(
                    "no usable compute environment on the job queue: "
                    + "; ".join(ce_reasons)
                )
        return problems

    def _validate_job_role(self) -> None:
        """Verify the configured job role exists (best-effort; needs iam:GetRole).

        A confirmed-missing role (``NoSuchEntity``) fails fast; anything else
        (most importantly a missing ``iam:GetRole`` permission) degrades silently
        so the check never blocks a workflow on uncertainty.
        """
        role_arn = getattr(self.settings, "job_role", None)
        if not role_arn or "/" not in role_arn:
            return
        # GetRole takes the bare role name, not the IAM path: for
        # arn:aws:iam::<acct>:role/<path>/<name> the name is the final segment.
        role_name = role_arn.rsplit("/", 1)[-1]
        try:
            boto3.client("iam", region_name=self.settings.region).get_role(
                RoleName=role_name
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchEntity":
                raise WorkflowError(
                    f"Configured AWS Batch job role does not exist: {role_arn}"
                ) from e
            self.logger.debug(
                f"could not verify job role (likely missing iam:GetRole): {e}"
            )
        except Exception as e:
            self.logger.debug(f"could not verify job role: {e}")
