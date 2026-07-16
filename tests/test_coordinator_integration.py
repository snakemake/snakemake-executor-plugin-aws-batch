"""End-to-end check that the coordinator mechanism actually submits one Batch
job and exits WITHOUT polling.

This exercises the exact native path the launcher relies on: a no-output wrapper
rule run with ``--executor aws-batch --immediate-submit --notemp``. AWS is fully
mocked — S3 via moto (default storage + source deployment), Batch via a stub
BatchClient — so it runs offline. Skipped if moto is not installed.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

moto = pytest.importorskip("moto")
import boto3  # noqa: E402

from snakemake_executor_plugin_aws_batch.coordinator import (  # noqa: E402
    COORDINATOR_RULE,
    build_coordinator_command,
    write_wrapper_snakefile,
)

BUCKET = "coordinator-it-bucket"


def _stub_batch_client(calls):
    client = MagicMock(name="BatchClient")
    client.register_job_definition.side_effect = lambda **kw: (
        calls.__setitem__("register", calls["register"] + 1)
        or {
            "jobDefinitionName": kw.get("jobDefinitionName", "jd"),
            "revision": 1,
            "jobDefinitionArn": "arn:aws:batch:us-east-1:0:job-definition/jd:1",
        }
    )
    client.submit_job.side_effect = lambda **kw: (
        calls.__setitem__("submit", calls["submit"] + 1)
        or {"jobName": kw.get("jobName", "j"), "jobId": "job-1", "jobQueue": "q"}
    )
    client.describe_jobs.side_effect = lambda **kw: (
        calls.__setitem__("describe", calls["describe"] + 1) or {"jobs": []}
    )
    client.deregister_job_definition.return_value = {}
    client.terminate_job.return_value = {}
    return client


@moto.mock_aws
def test_immediate_submit_submits_once_and_does_not_poll():
    os.environ.update(
        AWS_DEFAULT_REGION="us-east-1",
        AWS_ACCESS_KEY_ID="testing",
        AWS_SECRET_ACCESS_KEY="testing",
    )
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)

    from snakemake_executor_plugin_aws_batch.coordinator import CoordinatorConfig

    config = CoordinatorConfig(
        region="us-east-1",
        job_queue="arn:aws:batch:us-east-1:0:job-queue/workers",
        job_role="arn:aws:iam::0:role/r",
        default_storage_provider="s3",
        default_storage_prefix=f"s3://{BUCKET}/prefix",
    )

    workdir = Path(tempfile.mkdtemp(prefix="coord-it-"))
    # The wrapper `include:`s a real workflow, so it must exist and parse. The
    # coordinator job is mocked and never runs, so a trivial rule suffices; we
    # only verify the OUTER immediate-submit behavior.
    (workdir / "Snakefile").write_text("rule real_all:\n    shell: 'echo hi'\n")
    cmd = build_coordinator_command(config, ["-s", "Snakefile", "real_all"])
    wrapper = write_wrapper_snakefile(cmd, workdir, "Snakefile")
    os.chdir(workdir)

    from snakemake.api import SnakemakeApi
    from snakemake.settings.types import (
        DAGSettings,
        ExecutionSettings,
        RemoteExecutionSettings,
        ResourceSettings,
        StorageSettings,
    )
    from snakemake_executor_plugin_aws_batch import ExecutorSettings
    import snakemake_executor_plugin_aws_batch as plugin
    import snakemake_executor_plugin_aws_batch.batch_job_builder as bjb

    calls = {"register": 0, "submit": 0, "describe": 0}
    stub = _stub_batch_client(calls)

    with patch.object(plugin, "BatchClient", return_value=stub), patch.object(
        bjb, "BatchClient", return_value=stub
    ), patch.object(
        bjb.BatchJobBuilder, "_get_platform_from_queue", return_value="EC2"
    ):
        with SnakemakeApi() as api:
            wf = api.workflow(
                resource_settings=ResourceSettings(nodes=1),
                storage_settings=StorageSettings(
                    default_storage_provider="s3",
                    default_storage_prefix=f"s3://{BUCKET}/prefix",
                    notemp=True,
                ),
                snakefile=wrapper,
                workdir=workdir,
            )
            dag = wf.dag(dag_settings=DAGSettings(targets={COORDINATOR_RULE}))
            dag.execute_workflow(
                executor="aws-batch",
                execution_settings=ExecutionSettings(),
                remote_execution_settings=RemoteExecutionSettings(
                    immediate_submit=True,
                    seconds_between_status_checks=1,
                ),
                executor_settings=ExecutorSettings(
                    region="us-east-1",
                    job_queue=config.job_queue,
                    job_role=config.job_role,
                ),
            )

    # The coordinator job was submitted exactly once...
    assert calls["submit"] == 1
    # ...and immediate-submit did NOT enter a status-polling loop.
    assert calls["describe"] == 0
