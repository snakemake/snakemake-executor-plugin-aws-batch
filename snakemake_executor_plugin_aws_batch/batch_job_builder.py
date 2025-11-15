import uuid
from typing import List
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_executor_plugin_aws_batch.batch_client import BatchClient
from snakemake_executor_plugin_aws_batch.constant import (
    VALID_RESOURCES_MAPPING,
    BATCH_JOB_DEFINITION_TYPE,
    BATCH_JOB_PLATFORM_CAPABILITIES,
    BATCH_JOB_RESOURCE_REQUIREMENT_TYPE,
)


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
        # Determine platform from job queue
        self.platform = self._get_platform_from_queue()

    def _make_container_command(self, remote_command: str) -> List[str]:
        """
        Return docker CMD form of the command
        """
        return ["/bin/bash", "-c", remote_command]

    def _get_platform_from_queue(self) -> str:
        """
        Determine the platform (EC2 or FARGATE) from the job queue's compute environments.

        :return: Platform capability string (EC2 or FARGATE)
        """
        try:
            # Query the job queue
            queue_response = self.batch_client.describe_job_queues(
                jobQueues=[self.settings.job_queue]
            )

            if not queue_response.get("jobQueues"):
                self.logger.warning(
                    f"Job queue {self.settings.job_queue} not found. Defaulting to EC2."
                )
                return BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

            job_queue = queue_response["jobQueues"][0]
            compute_env_order = job_queue.get("computeEnvironmentOrder", [])

            if not compute_env_order:
                self.logger.warning(
                    f"No compute environments found for queue {self.settings.job_queue}. "
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
                    f"Compute environment {compute_env_arn} not found. Defaulting to EC2."
                )
                return BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

            compute_env = env_response["computeEnvironments"][0]

            # Check if it's a Fargate environment
            # Fargate environments have computeResources.type == "FARGATE" or "FARGATE_SPOT"
            compute_resources = compute_env.get("computeResources", {})
            resource_type = compute_resources.get("type", "")

            if resource_type in ["FARGATE", "FARGATE_SPOT"]:
                self.logger.info(
                    f"Detected FARGATE platform from queue {self.settings.job_queue}"
                )
                return BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value
            else:
                self.logger.info(
                    f"Detected EC2 platform from queue {self.settings.job_queue}"
                )
                return BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

        except Exception as e:
            self.logger.warning(
                f"Failed to determine platform from queue: {e}. Defaulting to EC2."
            )
            return BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

    def _validate_fargate_resources(self, vcpu: int, mem: int) -> tuple[str, str]:
        """Validates vcpu and memory conform to Fargate requirements.

        Fargate requires strict memory/vCPU combinations.
        https://docs.aws.amazon.com/batch/latest/userguide/fargate.html
        """
        if mem in VALID_RESOURCES_MAPPING:
            if vcpu in VALID_RESOURCES_MAPPING[mem]:
                return str(vcpu), str(mem)
            else:
                raise WorkflowError(f"Invalid vCPU value {vcpu} for memory {mem} MB on Fargate")
        else:
            min_mem = min([m for m, v in VALID_RESOURCES_MAPPING.items() if vcpu in v])
            self.logger.warning(
                f"Memory value {mem} MB is invalid for vCPU {vcpu} on Fargate. "
                f"Setting memory to minimum allowed value {min_mem} MB."
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
        job_uuid = str(uuid.uuid4())
        job_name = f"snakejob-{self.job.name}-{job_uuid}"
        job_definition_name = f"snakejob-def-{self.job.name}-{job_uuid}"

        # Validate and convert resources
        gpu = max(0, int(self.job.resources.get("_gpus", 0)))
        vcpu = max(1, int(self.job.resources.get("_cores", 1)))  # Default to 1 vCPU
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

        timeout = {"attemptDurationSeconds": self.settings.task_timeout}
        tags = self.settings.tags if isinstance(self.settings.tags, dict) else dict()
        try:
            job_def = self.batch_client.register_job_definition(
                jobDefinitionName=job_definition_name,
                type=BATCH_JOB_DEFINITION_TYPE.CONTAINER.value,
                containerProperties=container_properties,
                timeout=timeout,
                tags=tags,
                platformCapabilities=[self.platform],
            )
            self.created_job_defs.append(job_def)
            return job_def, job_name
        except Exception as e:
            raise WorkflowError(f"Failed to register job definition: {e}") from e

    def submit(self):
        job_def, job_name = self.build_job_definition()

        job_params = {
            "jobName": job_name,
            "jobQueue": self.settings.job_queue,
            "jobDefinition": "{}:{}".format(
                job_def["jobDefinitionName"], job_def["revision"]
            ),
        }

        try:
            submitted = self.batch_client.submit_job(**job_params)
            return submitted
        except Exception as e:
            raise WorkflowError(f"Failed to submit job: {e}") from e
