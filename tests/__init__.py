import os
from typing import Optional

import snakemake.common.tests
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase

from snakemake_executor_plugin_aws_batch import ExecutorSettings


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsMinioPlayStorageBase):
    def get_executor(self) -> str:
        return "aws-batch"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return ExecutorSettings(
            region=os.environ.get("SNAKEMAKE_AWS_BATCH_REGION", "us-west-2"),
            job_queue=os.environ.get("SNAKEMAKE_AWS_BATCH_JOB_QUEUE"),
            job_role=os.environ.get("SNAKEMAKE_AWS_BATCH_JOB_ROLE"),
        )

    def get_assume_shared_fs(self) -> bool:
        return False

    def get_remote_execution_settings(
        self,
    ) -> snakemake.settings.types.RemoteExecutionSettings:
        return snakemake.settings.types.RemoteExecutionSettings(
            seconds_between_status_checks=5,
            envvars=self.get_envvars(),
        )
