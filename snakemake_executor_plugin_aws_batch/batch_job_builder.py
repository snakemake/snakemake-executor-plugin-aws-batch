import uuid
from snakemake.exceptions import WorkflowError
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from enum import Enum
from snakemake_executor_plugin_aws_batch.batch_client import BatchClient

SNAKEMAKE_COMMAND = "snakemake"


class BATCH_JOB_DEFINITION_TYPE(Enum):
    CONTAINER = "container"
    MULTINODE = "multinode"


class BATCH_JOB_PLATFORM_CAPABILITIES(Enum):
    FARGATE = "FARGATE"
    EC2 = "EC2"


class BATCH_JOB_RESOURCE_REQUIREMENT_TYPE(Enum):
    GPU = "GPU"
    VCPU = "VCPU"
    MEMORY = "MEMORY"


class BatchJobBuilder:
    def __init__(
        self,
        logger,
        job: JobExecutorInterface,
        container_image: str,
        settings,
        job_command: str,
        batch_client: BatchClient,
    ):
        self.logger = logger
        self.job = job
        self.container_image = container_image
        self.settings = settings
        self.job_command = job_command
        self.batch_client = batch_client
        self.created_job_defs = []

    def build_job_definition(self):
        job_uuid = str(uuid.uuid4())
        job_name = f"snakejob-{self.job.name}-{job_uuid}"
        job_definition_name = f"snakejob-def-{self.job.name}-{job_uuid}"

        gpu = str(self.job.resources.get("_gpus", str(0)))
        vcpu = str(self.job.resources.get("_cores", str(1)))
        mem = str(self.job.resources.get("mem_mb", str(2048)))

        container_properties = {
            "image": self.container_image,
            "command": [self.job_command],
            "jobRoleArn": self.settings.job_role,
            "privileged": True,
            "resourceRequirements": [
                {"type": BATCH_JOB_RESOURCE_REQUIREMENT_TYPE.GPU.value, "value": gpu},
                {"type": BATCH_JOB_RESOURCE_REQUIREMENT_TYPE.VCPU.value, "value": vcpu},
                {
                    "type": BATCH_JOB_RESOURCE_REQUIREMENT_TYPE.MEMORY.value,
                    "value": mem,
                },  # noqa
            ],
        }

        timeout = dict()
        tags = self.settings.tags if isinstance(self.settings.tags, dict) else dict()
        try:
            job_def = self.batch_client.client.register_job_definition(
                jobDefinitionName=job_definition_name,
                type=BATCH_JOB_DEFINITION_TYPE.CONTAINER.value,
                containerProperties=container_properties,
                timeout=timeout,
                tags=tags,
                platformCapabilities=[BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value],
            )
            self.created_job_defs.append(job_def)
            return job_def, job_name
        except Exception as e:
            raise WorkflowError(e)

    def submit_job(self):
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
            self.logger.debug(
                "AWS Batch job submitted with queue {}, jobId {} and tags {}".format(
                    self.settings.job_queue, submitted["jobId"], self.settings.tags
                )
            )
            return submitted
        except Exception as e:
            raise WorkflowError(e)
