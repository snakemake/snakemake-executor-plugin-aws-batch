import os
import re
import uuid
from typing import Any, List, Optional
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

# AWS Batch name constraints
# Job names and job definition names must be <= 128 characters
# Valid characters: letters, numbers, hyphens, and underscores
  AWS_BATCH_MAX_NAME_LENGTH: int = 128
  # UUID (36) + "snakejob-def-" (13) + "-" (1) = 50 chars overhead for job def name
  _JOB_DEF_NAME_OVERHEAD: int = 50
  # Max rule name length accommodates both job and job def names
  MAX_RULE_NAME_LENGTH: int = AWS_BATCH_MAX_NAME_LENGTH - _JOB_DEF_NAME_OVERHEAD
# Suffix to indicate name was truncated (2 chars)
TRUNCATION_SUFFIX = "-x"


def _sanitize_job_name(name: str, max_length: int = MAX_RULE_NAME_LENGTH) -> str:
    """Sanitize a job name for AWS Batch compatibility.

    AWS Batch job names must:
    - Be <= 128 characters total
    - Contain only alphanumeric characters, hyphens, and underscores

    When names exceed max_length, they are truncated and suffixed with "-x"
    to indicate truncation occurred.

    Args:
        name: The raw job/rule name from Snakemake
        max_length: Maximum length for the sanitized name portion

    Returns:
        Sanitized name safe for AWS Batch
    """
    # Replace invalid characters with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    # Collapse multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    # Strip leading/trailing underscores or hyphens
    sanitized = sanitized.strip("_-")
    # Truncate to max length, leaving room for truncation suffix
    if len(sanitized) > max_length:
        truncate_at = max_length - len(TRUNCATION_SUFFIX)
        sanitized = sanitized[:truncate_at].rstrip("_-") + TRUNCATION_SUFFIX
    return sanitized or "job"


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
        # Platform (EC2/FARGATE) is resolved lazily on first access. Pre-existing
        # job definitions skip definition-building entirely, so they must not be
        # forced to hold batch:DescribeJobQueues / DescribeComputeEnvironments
        # permissions (the whole point of static definitions is a smaller IAM
        # surface). Only the dynamic registration path touches `self.platform`.
        self._platform: Optional[str] = None

    @property
    def platform(self) -> str:
        """The queue's platform capability (EC2/FARGATE), resolved on first use.

        Cached after the first lookup. Setting the attribute directly (e.g. in
        tests) bypasses the queue query.
        """
        if self._platform is None:
            self._platform = self._get_platform_from_queue()
        return self._platform

    @platform.setter
    def platform(self, value: str) -> None:
        self._platform = value

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
        sanitized_name = _sanitize_job_name(self.job.name)
        job_name = f"snakejob-{sanitized_name}-{job_uuid}"
        job_definition_name = f"snakejob-def-{sanitized_name}-{job_uuid}"

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

        # Only include `timeout` when a timeout is explicitly set — omitting the
        # key means AWS Batch applies no timeout, which is the correct default for
        # long-running bioinformatics workloads.
        task_timeout = self._resolve_task_timeout()
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

    def _resolve_task_timeout(self) -> Optional[int]:
        """Resolve the effective task timeout in seconds, or ``None``.

        The per-rule ``aws_batch_task_timeout`` resource takes precedence over
        the workflow-level ``task_timeout`` setting. Returns ``None`` when neither
        is set so the ``timeout`` field can be omitted entirely (AWS Batch then
        applies no timeout). Enforces the AWS minimum of 60 s locally so the error
        is clear rather than a Batch API rejection. Raises ``WorkflowError`` for
        non-integer or sub-minimum values.
        """
        rule_timeout = self.job.resources.get("aws_batch_task_timeout")
        if rule_timeout is not None:
            task_timeout = rule_timeout
            timeout_source = "aws_batch_task_timeout resource"
        else:
            task_timeout = getattr(self.settings, "task_timeout", None)
            timeout_source = "--aws-batch-task-timeout"
        if task_timeout is None:
            return None
        try:
            task_timeout = int(task_timeout)
        except (TypeError, ValueError) as e:
            raise WorkflowError(
                f"Invalid {timeout_source} value {task_timeout!r}: "
                "must be an integer number of seconds (minimum 60)."
            ) from e
        if task_timeout < 60:
            raise WorkflowError(
                f"{timeout_source} must be at least 60 seconds (AWS minimum), "
                f"got {task_timeout}."
            )
        return task_timeout

    def _resolve_scheduling_priority(self) -> Optional[int]:
        """Resolve the effective scheduling priority override, or ``None``.

        For fair-share queues. The per-rule ``aws_batch_scheduling_priority``
        resource takes precedence over the workflow-level ``scheduling_priority``
        setting. Returns ``None`` when neither is set so the
        ``schedulingPriorityOverride`` kwarg can be omitted entirely — keeping
        submissions byte-identical on non-fair-share queues. Raises
        ``WorkflowError`` for non-integer or out-of-range values.
        """
        resource_priority = self.job.resources.get("aws_batch_scheduling_priority")
        setting_priority = getattr(self.settings, "scheduling_priority", None)
        if resource_priority is not None:
            priority = resource_priority
            priority_source = "aws_batch_scheduling_priority resource"
        elif setting_priority is not None:
            priority = setting_priority
            priority_source = "--aws-batch-scheduling-priority setting"
        else:
            return None
        try:
            priority_int = int(priority)
        except (TypeError, ValueError) as e:
            raise WorkflowError(
                f"Invalid {priority_source} {priority!r}: must be an integer."
            ) from e
        if not (0 <= priority_int <= 9999):
            raise WorkflowError(
                f"Invalid {priority_source} {priority_int}: "
                "must be in range [0, 9999]."
            )
        return priority_int

    def _resolve_preexisting_job_definition(self) -> Optional[str]:
        """Return the effective pre-existing job definition name/ARN, or None.

        Checks the per-rule ``aws_batch_job_definition`` resource first (mirrors
        the ``batch_queue`` override pattern), then falls back to the
        ``job_definition`` executor setting.  Returns ``None`` when neither is
        set, which triggers the dynamic registration path.
        """
        per_rule = self.job.resources.get("aws_batch_job_definition")
        if per_rule:
            return str(per_rule)
        return getattr(self.settings, "job_definition", None) or None

    def _validate_preexisting_compatibility(self) -> None:
        """Fail fast when settings that only make sense for dynamic definitions are set.

        ``job_role`` is baked into the job definition at registration time and
        cannot be overridden via ``containerOverrides``.  ``shared_memory_size_mb``
        maps to ``linuxParameters.sharedMemorySize`` on the definition, not a
        container override.  Both are silently useless in pre-existing mode, so we
        raise early rather than silently ignore.

        ``container_image`` cannot be distinguished from its default value so we
        document in the setting help text that it is ignored and do not raise here.
        """
        if getattr(self.settings, "job_role", None):
            raise WorkflowError(
                "Cannot combine job_role (--aws-batch-job-role) with job_definition "
                "(--aws-batch-job-definition): the job role is managed externally when "
                "using a pre-existing job definition.  Remove --aws-batch-job-role or "
                "omit --aws-batch-job-definition."
            )
        if self.job.resources.get("shared_memory_size_mb") is not None:
            raise WorkflowError(
                "Cannot combine the shared_memory_size_mb resource with "
                "--aws-batch-job-definition: shared memory sizing is managed "
                "externally when using a pre-existing job definition.  Remove the "
                "shared_memory_size_mb resource or omit --aws-batch-job-definition."
            )

    def _submit_with_preexisting_definition(self, job_definition: str) -> dict:
        """Submit a job using a pre-existing definition via containerOverrides.

        Skips ``register_job_definition`` entirely.  Per-job specifics — command,
        environment variables, vcpu/mem, and (when > 0) GPU — are passed through
        ``containerOverrides`` so the definition's other settings remain in force.

        Fargate/vcpu-mem constraint validation is intentionally skipped here:
        the user-owned job definition already encodes the platform requirements,
        and enforcing them here would duplicate logic that AWS validates at
        submit time.

        Returns the ``submit_job`` response dict with an extra
        ``_preexisting_job_definition`` marker so ``_deregister_job`` knows to
        skip deregistration.
        """
        self._validate_preexisting_compatibility()

        job_uuid = str(uuid.uuid4())
        sanitized_name = _sanitize_job_name(self.job.name)
        job_name = f"snakejob-{sanitized_name}-{job_uuid}"

        gpu = max(0, int(self.job.resources.get("_gpus", 0)))
        vcpu = max(
            1,
            (
                self.job.threads
                if self.job.threads > 0
                else int(self.job.resources.get("_cores", 1))
            ),
        )
        mem = max(1, int(self.job.resources.get("mem_mb", 1024)))

        resource_requirements = [
            {
                "type": BATCH_JOB_RESOURCE_REQUIREMENT_TYPE.VCPU.value,
                "value": str(vcpu),
            },
            {
                "type": BATCH_JOB_RESOURCE_REQUIREMENT_TYPE.MEMORY.value,
                "value": str(mem),
            },
        ]
        if gpu > 0:
            resource_requirements.append(
                {
                    "type": BATCH_JOB_RESOURCE_REQUIREMENT_TYPE.GPU.value,
                    "value": str(gpu),
                }
            )

        container_overrides: dict = {
            "command": self._make_container_command(self.job_command),
            "resourceRequirements": resource_requirements,
        }
        if self.envvars:
            container_overrides["environment"] = [
                {"name": k, "value": v} for k, v in self.envvars.items()
            ]

        job_params: dict = {
            "jobName": job_name,
            "jobQueue": self.job_queue,
            "jobDefinition": job_definition,
            "containerOverrides": container_overrides,
        }

        tags = self._build_job_tags()
        if tags:
            job_params["tags"] = tags

        # Mirror the optional kwargs from the dynamic path. The dynamic path bakes
        # the timeout into the registered definition; here we have no definition to
        # register, so the timeout travels as SubmitJob's top-level `timeout`
        # field, which overrides any timeout on the pre-existing definition.
        # Scheduling priority applies equally to pre-existing definitions.
        # NOTE: any future kwarg added to submit() (e.g. propagateTags) must also
        # be mirrored here.
        task_timeout = self._resolve_task_timeout()
        if task_timeout is not None:
            job_params["timeout"] = {"attemptDurationSeconds": task_timeout}

        priority = self._resolve_scheduling_priority()
        if priority is not None:
            job_params["schedulingPriorityOverride"] = priority

        try:
            submitted = self.batch_client.submit_job(**job_params)
            submitted["_preexisting_job_definition"] = True
            return submitted
        except Exception as e:
            raise WorkflowError(f"Failed to submit job: {e}") from e

    def submit(self):
        preexisting = self._resolve_preexisting_job_definition()
        if preexisting:
            return self._submit_with_preexisting_definition(preexisting)

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

        priority = self._resolve_scheduling_priority()
        if priority is not None:
            job_params["schedulingPriorityOverride"] = priority

        try:
            submitted = self.batch_client.submit_job(**job_params)
            return submitted
        except Exception as e:
            raise WorkflowError(f"Failed to submit job: {e}") from e
