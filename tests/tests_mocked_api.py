from unittest.mock import AsyncMock, patch
from tests import TestWorkflowsBase


class TestWorkflowsMocked(TestWorkflowsBase):
    __test__ = True

    @patch(
        # BatchJobBuilder.__init__ calls _get_platform_from_queue(), which issues
        # live describe_job_queues / describe_compute_environments calls. Without
        # AWS credentials (as in CI) these raise NoCredentialsError before submit
        # is ever reached, so short-circuit platform detection to EC2.
        "snakemake_executor_plugin_aws_batch.batch_job_builder."
        "BatchJobBuilder._get_platform_from_queue",
        return_value="EC2",
    )
    @patch(
        "snakemake_executor_plugin_aws_batch.batch_job_builder.BatchJobBuilder.submit",
        return_value={"jobName": "job_id", "jobId": "job_id", "jobQueue": "job_queue"},
    )
    @patch(
        "snakemake_executor_plugin_aws_batch.Executor._get_job_status",
        return_value=(0, "SUCCEEDED"),
    )
    @patch(
        "snakemake.dag.DAG.check_and_touch_output",
        new=AsyncMock(autospec=True),
    )
    @patch(
        "snakemake_storage_plugin_s3.StorageObject.managed_size",
        new=AsyncMock(autospec=True, return_value=0),
    )
    @patch(
        "snakemake.jobs.wait_for_files",
        new=AsyncMock(autospec=True),
    )
    def run_workflow(
        self, test_name, tmp_path, deployment_method=frozenset(), *extra_args, **kwargs
    ):
        super().run_workflow(test_name, tmp_path, deployment_method=deployment_method)
