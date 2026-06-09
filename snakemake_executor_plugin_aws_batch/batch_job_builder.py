import os
import uuid
from typing import Any, List
from botocore.exceptions import ClientError
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_executor_plugin_aws_batch.batch_client import BatchClient
from snakemake_executor_plugin_aws_batch.constant import (
    VALID_RESOURCES_MAPPING,
    BATCH_JOB_DEFINITION_TYPE,
    BATCH_JOB_PLATFORM_CAPABILITIES,
    BATCH_JOB_RESOURCE_REQUIREMENT_TYPE,
)

SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR = "SNAKEMAKE_AWS_BATCH_JOB_TAGS"


class BatchJobBuilder:
    def __init__(
        self,
        logger,
        job: JobExecutorInterface,
        envvars: dict,
        container_image: str,
        settings,
        job_command: str,
        batch_client: BatchClient,
    ):
        self.logger = logger
        self.job = job
        self.envvars = envvars
        self.container_image = container_image
        self.settings = settings
        self.job_command = job_command
        self.batch_client = batch_client
        self.created_job_defs = []
        # Per-rule queue override via `resources.batch_queue` — lets a multi-arch
        # workflow route each job to a queue wired to matching compute environment
        # (e.g. ARM vs x86). Falls back to the profile-wide default queue.
        self.job_queue = str(
            self.job.resources.get("batch_queue", self.settings.job_queue)
        )
        # Determine platform from job queue
        self.platform = self._get_platform_from_queue()

    def _make_container_command(self, remote_command: str) -> List[str]:
        """
        Return docker CMD form of the command
        """
        return ["/bin/bash", "-c", remote_command]

    def _get_platform_from_queue(self) -> str:
        """
        Determine the platform (EC2 or FARGATE) from the job queue's
        compute environments.

        :return: Platform capability string (EC2 or FARGATE)
        """
        try:
            # Query the job queue
            queue_response = self.batch_client.describe_job_queues(
                jobQueues=[self.job_queue]
            )

            if not queue_response.get("jobQueues"):
                self.logger.warning(
                    f"Job queue {self.job_queue} not found. Defaulting to EC2."
                )
                return BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

            job_queue = queue_response["jobQueues"][0]
            compute_env_order = job_queue.get("computeEnvironmentOrder", [])

            if not compute_env_order:
                self.logger.warning(
                    f"No compute environments found for queue {self.job_queue}. "
                    "Defaulting to EC2."
                )
                return BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

            # Get the first compute environment ARN
            compute_env_arn = compute_env_order[0]["computeEnvironment"]

            # Query the compute environment to get its type
            env_response = self.batch_client.describe_compute_environments(
                computeEnvironments=[compute_env_arn]
            )

            if not env_response.get("computeEnvironments"):
                self.logger.warning(
                    f"Compute environment {compute_env_arn} not found. "
                    "Defaulting to EC2."
                )
                return BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

            compute_env = env_response["computeEnvironments"][0]

            # Check if it's a Fargate environment
            # Fargate environments have computeResources.type ==
            # "FARGATE" or "FARGATE_SPOT"
            compute_resources = compute_env.get("computeResources", {})
            resource_type = compute_resources.get("type", "")

            if resource_type in ["FARGATE", "FARGATE_SPOT"]:
                self.logger.info(
                    f"Detected FARGATE platform from queue {self.job_queue}"
                )
                return BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value
            else:
                self.logger.info(f"Detected EC2 platform from queue {self.job_queue}")
                return BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

        except ClientError as e:
            # Real AWS errors (permissions, invalid queue, throttling) must
            # surface immediately. The EC2 fallback is reserved for the
            # explicit empty-response branches above where AWS replied
            # successfully but the resource is missing.
            raise WorkflowError(
                f"Failed to determine platform from queue " f"{self.job_queue}: {e}"
            ) from e

    def _validate_fargate_resources(self, vcpu: int, mem: int) -> tuple[str, str]:
        """Validates vcpu and memory conform to Fargate requirements.

        Fargate requires strict memory/vCPU combinations.
        https://docs.aws.amazon.com/batch/latest/userguide/fargate.html
        """
        if mem in VALID_RESOURCES_MAPPING:
            if vcpu in VALID_RESOURCES_MAPPING[mem]:
                return str(vcpu), str(mem)
            else:
                raise WorkflowError(
                    f"Invalid vCPU value {vcpu} for memory {mem} MB on Fargate"
                )
        else:
            valid_mems = [m for m, v in VALID_RESOURCES_MAPPING.items() if vcpu in v]
            if not valid_mems:
                raise WorkflowError(
                    f"Invalid vCPU value {vcpu} for Fargate. "
                    f"Check valid Fargate resource configurations."
                )
            # Pick the smallest valid memory that still satisfies the request.
            # Picking min(valid_mems) unconditionally would silently shrink
            # e.g. mem=5000 down to 2048 and OOM the job.
            candidate_mems = [m for m in valid_mems if m >= mem]
            if not candidate_mems:
                raise WorkflowError(
                    f"Memory value {mem} MB exceeds the maximum allowed for "
                    f"vCPU {vcpu} on Fargate."
                )
            min_mem = min(candidate_mems)
            self.logger.warning(
                f"Memory value {mem} MB is invalid for vCPU {vcpu} on Fargate. "
                f"Setting memory to next allowed value {min_mem} MB."
            )
            return str(vcpu), str(min_mem)

    def _validate_ec2_resources(self, vcpu: int, mem: int) -> tuple[str, str]:
        """Validates vcpu and memory for EC2 compute environments.

        EC2 allows flexible resource allocation - just basic sanity checks.
        https://docs.aws.amazon.com/batch/latest/userguide/compute_environment_parameters.html
        """
        if vcpu < 1:
            raise WorkflowError(f"vCPU must be at least 1, got {vcpu}")
        if mem < 1024:
            raise WorkflowError(f"Memory must be at least 1024 MiB, got {mem} MiB")
        return str(vcpu), str(mem)

    def _validate_resources(self, vcpu: str, mem: str) -> tuple[str, str]:
        """Validates vcpu and memory based on platform requirements.

        https://docs.aws.amazon.com/batch/latest/APIReference/API_ResourceRequirement.html
        """
        vcpu_int = int(vcpu)
        mem_int = int(mem)

        if self.platform == BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value:
            return self._validate_fargate_resources(vcpu_int, mem_int)
        else:
            return self._validate_ec2_resources(vcpu_int, mem_int)

    def build_job_definition(self):
        # Fargate requires platform-specific container properties this plugin
        # does not yet emit (executionRoleArn is mandatory; privileged mode
        # and GPU resources are unsupported). Without those changes the
        # register_job_definition call would be rejected by AWS. Fail fast
        # here with a clear message until Fargate support is wired through.
        if self.platform == BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value:
            raise WorkflowError(
                f"Fargate job definitions are not supported by this plugin "
                f"(queue {self.job_queue} resolves to FARGATE). "
                f"Use an EC2-backed AWS Batch queue instead."
            )

        job_uuid = str(uuid.uuid4())
        job_name = f"snakejob-{self.job.name}-{job_uuid}"
        job_definition_name = f"snakejob-def-{self.job.name}-{job_uuid}"

        # Validate and convert resources
        gpu = max(0, int(self.job.resources.get("_gpus", 0)))
        # Use threads directive, fall back to _cores for backward compatibility
        vcpu = max(
            1,
            self.job.threads
            if self.job.threads > 0
            else int(self.job.resources.get("_cores", 1)),
        )
        mem = max(1, int(self.job.resources.get("mem_mb", 1024)))  # Default to 1024 MiB

        vcpu_str, mem_str = self._validate_resources(str(vcpu), str(mem))
        gpu_str = str(gpu)

        environment = []
        if self.envvars:
            environment = [{"name": k, "value": v} for k, v in self.envvars.items()]

        container_properties = {
            "image": self.container_image,
            # command requires a list of strings (docker CMD format)
            "command": self._make_container_command(self.job_command),
            "environment": environment,
            "jobRoleArn": self.settings.job_role,
            "privileged": True,
            "resourceRequirements": [
                {
                    "type": BATCH_JOB_RESOURCE_REQUIREMENT_TYPE.VCPU.value,
                    "value": vcpu_str,
                },
                {
                    "type": BATCH_JOB_RESOURCE_REQUIREMENT_TYPE.MEMORY.value,
                    "value": mem_str,
                },
            ],
        }

        if gpu > 0:
            container_properties["resourceRequirements"].append(
                {
                    "type": BATCH_JOB_RESOURCE_REQUIREMENT_TYPE.GPU.value,
                    "value": gpu_str,
                }
            )

        # Optional /dev/shm sizing. POSIX shm_open writes there; the EC2/ECS
        # default is 64 MB which is too small for tools that stage large
        # in-memory indexes (e.g. bwa-mem2 shm). Rules opt in via
        # `resources.shared_memory_size_mb`. Only applies on EC2 — Fargate
        # does not honor linuxParameters.sharedMemorySize.
        shm_size_mb = self.job.resources.get("shared_memory_size_mb")
        if shm_size_mb and self.platform == BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value:
            try:
                shm_size = int(shm_size_mb)
            except (TypeError, ValueError) as e:
                raise WorkflowError(
                    f"Invalid shared_memory_size_mb resource {shm_size_mb!r}: "
                    f"must be an integer number of MiB."
                ) from e
            if shm_size <= 0:
                raise WorkflowError(
                    f"Invalid shared_memory_size_mb resource {shm_size}: "
                    f"must be a positive number of MiB."
                )
            container_properties["linuxParameters"] = {
                "sharedMemorySize": shm_size,
            }

        # Only include `timeout` when task_timeout is explicitly set — omitting the
        # key means AWS Batch applies no timeout, which is the correct default for
        # long-running bioinformatics workloads. When set, enforce the AWS minimum
        # of 60 s locally so the error is clear rather than a Batch API rejection.
        task_timeout = self.settings.task_timeout
        if task_timeout is not None and task_timeout < 60:
            raise WorkflowError(
                f"--aws-batch-task-timeout must be at least 60 seconds (AWS minimum), "
                f"got {task_timeout}."
            )
        # Use the same validated, env-merged tag set as submit_job so the job
        # definition carries identical tags (each job registers its own
        # definition, so per-run cost-tracking tags apply to both).
        tags = self._build_job_tags()
        register_kwargs: dict[str, Any] = dict(
            jobDefinitionName=job_definition_name,
            type=BATCH_JOB_DEFINITION_TYPE.CONTAINER.value,
            containerProperties=container_properties,
            tags=tags,
            platformCapabilities=[self.platform],
        )
        if task_timeout is not None:
            register_kwargs["timeout"] = {"attemptDurationSeconds": task_timeout}
        try:
            job_def = self.batch_client.register_job_definition(**register_kwargs)
            self.created_job_defs.append(job_def)
            return job_def, job_name
        except Exception as e:
            raise WorkflowError(f"Failed to register job definition: {e}") from e

    def _build_job_tags(self) -> dict:
        """Build the tags dict for job submission.

        Merges tags from settings with those from the SNAKEMAKE_AWS_BATCH_JOB_TAGS
        environment variable (comma-separated KEY=VALUE pairs). Environment variable
        tags take precedence over settings tags on key conflicts.

        Malformed env input is rejected with a WorkflowError: empty keys
        (``=value``) and pairs without ``=`` (``value``). Empty pairs from
        trailing or doubled commas are tolerated. AWS Batch caps a job at 50
        tags; exceeding that also raises locally so the job fails fast
        instead of at submit_job time.

        :return: Merged tags dict (may be empty).
        """
        tags: dict = (
            dict(self.settings.tags) if isinstance(self.settings.tags, dict) else {}
        )
        for key in tags:
            if not key or not key.strip():
                raise WorkflowError(
                    "Invalid tags in settings: tag key cannot be empty."
                )

        env_tags_str = os.environ.get(SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR, "")
        if env_tags_str:
            for pair in env_tags_str.split(","):
                pair = pair.strip()
                if not pair:
                    # Tolerate trailing or doubled commas.
                    continue
                if "=" not in pair:
                    raise WorkflowError(
                        f"Invalid {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR}: "
                        f"malformed pair {pair!r} (expected KEY=VALUE)."
                    )
                key, _, value = pair.partition("=")
                key = key.strip()
                if not key:
                    raise WorkflowError(
                        f"Invalid {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR}: "
                        "tag key cannot be empty."
                    )
                tags[key] = value.strip()

        if len(tags) > 50:
            raise WorkflowError(
                f"AWS Batch jobs support at most 50 tags, got {len(tags)}."
            )

        return tags

    def submit(self):
        job_def, job_name = self.build_job_definition()

        job_params = {
            "jobName": job_name,
            "jobQueue": self.job_queue,
            "jobDefinition": "{}:{}".format(
                job_def["jobDefinitionName"], job_def["revision"]
            ),
        }

        tags = self._build_job_tags()
        if tags:
            job_params["tags"] = tags

        try:
            submitted = self.batch_client.submit_job(**job_params)
            return submitted
        except Exception as e:
            raise WorkflowError(f"Failed to submit job: {e}") from e
