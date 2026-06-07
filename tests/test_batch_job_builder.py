"""Tests for BatchJobBuilder — task_timeout behaviour."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_executor_plugin_aws_batch.batch_job_builder import BatchJobBuilder


def _fake_job_def(name="snakejob-def-test"):
    return {
        "jobDefinitionName": name,
        "jobDefinitionArn": (
            f"arn:aws:batch:us-east-1:123456789:job-definition/{name}:1"
        ),
        "revision": 1,
        "status": "ACTIVE",
        "type": "container",
    }


def _make_builder(task_timeout) -> BatchJobBuilder:
    """Return a BatchJobBuilder with the given task_timeout setting."""
    settings = SimpleNamespace(
        job_queue="test-queue",
        job_role="arn:aws:iam::123456789:role/test-role",
        tags=None,
        task_timeout=task_timeout,
    )
    batch_client = MagicMock()
    batch_client.register_job_definition.return_value = _fake_job_def()

    job = MagicMock()
    job.name = "test_rule"
    job.resources = {"_cores": 1, "mem_mb": 1024}

    return BatchJobBuilder(
        logger=MagicMock(),
        job=job,
        envvars={},
        container_image="test-image:latest",
        settings=settings,
        job_command="snakemake ...",
        batch_client=batch_client,
    )


class TestTaskTimeout:
    def test_default_none_omits_timeout_from_register_kwargs(self):
        """None task_timeout: register_job_definition must not receive a timeout key."""
        builder = _make_builder(None)
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert "timeout" not in call_kwargs

    def test_set_timeout_includes_timeout_in_register_kwargs(self):
        """Set task_timeout: register_job_definition must receive the timeout dict."""
        builder = _make_builder(3600)
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 3600}

    def test_minimum_valid_timeout_60_passes(self):
        """60 seconds is the AWS minimum; it must not raise."""
        builder = _make_builder(60)
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 60}

    def test_timeout_below_60_raises_workflow_error(self):
        """task_timeout < 60 must raise WorkflowError before calling the API."""
        builder = _make_builder(59)
        with pytest.raises(WorkflowError, match="60"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()

    def test_timeout_of_1_raises_workflow_error(self):
        """Any value below 60 must be rejected."""
        builder = _make_builder(1)
        with pytest.raises(WorkflowError, match="60"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()

    def test_timeout_of_0_raises_workflow_error(self):
        """Zero is below the AWS minimum; must raise WorkflowError."""
        builder = _make_builder(0)
        with pytest.raises(WorkflowError, match="60"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()

    def test_timeout_negative_raises_workflow_error(self):
        """Negative values are below the AWS minimum; must raise WorkflowError."""
        builder = _make_builder(-1)
        with pytest.raises(WorkflowError, match="60"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()
