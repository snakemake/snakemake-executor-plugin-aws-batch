import os
import uuid
from typing import Optional, Mapping

import snakemake.common.tests
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase
from snakemake_executor_plugin_aws_batch import ExecutorSettings
from snakemake.common.tests import TestWorkflowsBase as TestWorkflowsBase
from snakemake_interface_common.plugin_registry.plugin import TaggedSettings
from snakemake_interface_common.utils import lazy_property


class TestWorkflowsMinioLocalStorageBase(TestWorkflowsBase):
    def get_default_storage_provider(self) -> Optional[str]:
        return "s3"

    def get_default_storage_prefix(self) -> Optional[str]:
        return f"s3://{self.bucket}"

    def get_default_storage_provider_settings(
        self,
    ) -> Optional[Mapping[str, TaggedSettings]]:
        from snakemake_storage_plugin_s3 import StorageProviderSettings

        self._storage_provider_settings = StorageProviderSettings(
            endpoint_url=self.endpoint_url,
            access_key=self.access_key,
            secret_key=self.secret_key,
        )

        tagged_settings = TaggedSettings()
        tagged_settings.register_settings(self._storage_provider_settings)
        return {"s3": tagged_settings}

    def cleanup_test(self):
        import boto3

        # clean up using boto3
        s3c = boto3.resource(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        try:
            s3c.Bucket(self.bucket).delete()
        except Exception:
            pass

    @lazy_property
    def bucket(self):
        return f"snakemake-{uuid.uuid4().hex}"

    @property
    def endpoint_url(self):
        return "http://127.0.0.1:9000"

    @property
    def access_key(self):
        return "minio"

    @property
    def secret_key(self):
        return "minio123"


class TestWorkflowsBase(TestWorkflowsMinioLocalStorageBase):
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
