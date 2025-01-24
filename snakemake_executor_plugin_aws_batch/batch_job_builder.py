import uuid
from batch_client import BatchClient
from snakemake.exceptions import WorkflowError
import shlex
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from enum import Enum

class BATCH_JOB_DEFINITION_TYPE(Enum):
    CONTAINER = "container"
    MULTINODE = "multinode"

class BATCH_JOB_PLATFORM_CAPABILITIES(Enum):
    FARGATE = "FARGATE"
    EC2 = "EC2"

class BatchJobBuilder:
    def __init__(self, job, container_image, settings, batch_client=None):
        self.job = job
        self.container_image = container_image
        self.settings = settings
        self.batch_client = batch_client or BatchClient()
        self.created_job_defs = []

    def build_job_definition(self):
        job_uuid = str(uuid.uuid4())
        job_name = f"snakejob-{self.job.name}-{job_uuid}"
        job_definition_name = f"snakejob-def-{self.job.name}-{job_uuid}"

        vcpu = str(self.job.resources.get("_cores", str(1)))
        mem = str(self.job.resources.get("mem_mb", str(2048)))

        container_properties = {
            "command": ["snakemake"],
            "image": self.container_image,
            "privileged": True,
            "resourceRequirements": [
                {"type": "VCPU", "value": vcpu},
                {"type": "MEMORY", "value": mem},
            ],
            "networkConfiguration": {
                "assignPublicIp": "ENABLED",
            },
            "executionRoleArn": self.settings.execution_role,
        }

        tags = self.settings.tags if isinstance(self.settings.tags, dict) else dict()
        try:
            job_def = self.batch_client.client.register_job_definition(
                jobDefinitionName=job_definition_name,
                type=BATCH_JOB_DEFINITION_TYPE.CONTAINER.value,
                containerProperties=container_properties,
                platformCapabilities=[BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value],
                tags=tags,
            )
            self.created_job_defs.append(job_def)
            return job_def, job_name
        except Exception as e:
            raise WorkflowError(e)

    def _generate_snakemake_command(self, job: JobExecutorInterface) -> str:
        """generates the snakemake command for the job"""
        exec_job = self.format_job_exec(job)
        return ["sh", "-c", shlex.quote(exec_job)]

    def submit_job(self):
        job_def, job_name = self.build_job_definition()
        job_command = self._generate_snakemake_command(self.job)

        job_params = {
            "jobName": job_name,
            "jobQueue": self.settings.job_queue,
            "jobDefinition": "{}:{}".format(
                job_def["jobDefinitionName"], job_def["revision"]
            ),
            "containerOverrides": {
                "command": job_command,
                "resourceRequirements": [
                    {"type": "VCPU", "value": str(self.job.resources.get("_cores", str(1)))},
                    {"type": "MEMORY", "value": str(self.job.resources.get("mem_mb", str(2048)))},
                ],
            },
        }

        if self.settings.tags:
            job_params["tags"] = self.settings.tags

        if self.settings.task_timeout is not None:
            job_params["timeout"] = {
                "attemptDurationSeconds": self.settings.task_timeout
            }

        try:
            submitted = self.batch_client.client.submit_job(**job_params)
            self.logger.debug(
                "AWS Batch job submitted with queue {}, jobId {} and tags {}".format(
                    self.settings.job_queue, submitted["jobId"], self.settings.tags
                )
            )
            return submitted
        except Exception as e:
            raise WorkflowError(e)
