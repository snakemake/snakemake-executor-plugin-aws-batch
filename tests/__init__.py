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
            region=os.environ.get("SNAKEMAKE_AWS_BATCH_REGION", "us-east-1"),
            job_queue=os.environ.get("SNAKEMAKE_AWS_BATCH_JOB_QUEUE"),
            execution_role=os.environ.get("SNAKEMAKE_AWS_BATCH_EXECUTION_ROLE"),
        )

    def get_assume_shared_fs(self) -> bool:
        return False

    def get_remote_execution_settings(
        self,
    ) -> snakemake.settings.RemoteExecutionSettings:
        return snakemake.settings.RemoteExecutionSettings(
            seconds_between_status_checks=5,
            envvars=self.get_envvars(),
        )
