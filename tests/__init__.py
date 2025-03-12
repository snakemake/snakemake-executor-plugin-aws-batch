import os

import snakemake.common.tests
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase
from snakemake_executor_plugin_aws_batch import ExecutorSettings
from snakemake.common.tests import TestWorkflowsMinioPlayStorageBase
from typing import Optional
from snakemake_interface_common.plugin_registry.plugin import TaggedSettings
from typing import Mapping


class TestWorkflowsBase(TestWorkflowsMinioPlayStorageBase):
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


class TestWorkflowsTrueStorageBase(TestWorkflowsBase):
    def get_default_storage_provider_settings(
        self,
    ) -> Optional[Mapping[str, TaggedSettings]]:
        from snakemake_storage_plugin_s3 import StorageProviderSettings

        self._storage_provider_settings = StorageProviderSettings(
            access_key=os.getenv("SNAKEMAKE_STORAGE_S3_ACCESS_KEY"),
            secret_key=os.getenv("SNAKEMAKE_STORAGE_S3_SECRET_KEY"),
        )

        tagged_settings = TaggedSettings()
        tagged_settings.register_settings(self._storage_provider_settings)
        return {"s3": tagged_settings}
