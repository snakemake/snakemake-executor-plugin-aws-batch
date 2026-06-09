"""Unit tests for BatchJobBuilder.

Covers tag propagation, platform detection error handling, Fargate resource
validation, and Fargate rejection in build_job_definition. All tests run with
mocked AWS clients — no AWS credentials required.
"""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_aws_batch.batch_job_builder import (
    SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR,
    BatchJobBuilder,
)
from snakemake_executor_plugin_aws_batch.constant import (
    BATCH_JOB_PLATFORM_CAPABILITIES,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_builder(tags=None) -> BatchJobBuilder:
    """Return a BatchJobBuilder with minimal mocks.

    The batch_client is fully mocked so no AWS calls are made.  build_job_definition
    is patched in each test that exercises submit() so we only test the tag-assembly
    logic in isolation.
    """
    settings = SimpleNamespace(
        job_queue="test-queue",
        job_role="arn:aws:iam::123456789:role/test-role",
        tags=tags,
        task_timeout=300,
    )

    batch_client = MagicMock()
    # _get_platform_from_queue is called during __init__; short-circuit it.
    batch_client.describe_job_queues.return_value = {"jobQueues": []}

    logger = MagicMock()
    job = MagicMock()
    job.name = "test_rule"
    job.threads = 1
    job.resources = {"_cores": 1, "mem_mb": 1024}

    builder = BatchJobBuilder(
        logger=logger,
        job=job,
        envvars={},
        container_image="test-image:latest",
        settings=settings,
        job_command="snakemake ...",
        batch_client=batch_client,
    )
    return builder


def _fake_job_def():
    """Return a minimal job-definition response for build_job_definition mocking."""
    return {"jobDefinitionName": "snakejob-def-test", "revision": 1}


# ---------------------------------------------------------------------------
# Tests for _build_job_tags
# ---------------------------------------------------------------------------


class TestBuildJobTags:
    def test_none_settings_tags_returns_empty(self):
        builder = _make_builder(tags=None)
        assert builder._build_job_tags() == {}

    def test_empty_dict_settings_tags_returns_empty(self):
        builder = _make_builder(tags={})
        assert builder._build_job_tags() == {}

    def test_settings_tags_included(self):
        builder = _make_builder(tags={"Env": "prod", "Project": "fgumi"})
        result = builder._build_job_tags()
        assert result == {"Env": "prod", "Project": "fgumi"}

    def test_settings_tags_not_mutated(self):
        """_build_job_tags must return a copy, not mutate settings.tags."""
        original = {"Env": "prod"}
        builder = _make_builder(tags=original)
        result = builder._build_job_tags()
        result["Extra"] = "value"
        assert "Extra" not in original

    def test_env_var_tags_parsed_and_merged(self):
        builder = _make_builder(tags={"Env": "prod"})
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Team=data,Cost=low"}
        ):
            result = builder._build_job_tags()
        assert result == {"Env": "prod", "Team": "data", "Cost": "low"}

    def test_env_var_tags_override_settings_tags_on_conflict(self):
        builder = _make_builder(tags={"Env": "prod", "Team": "bio"})
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Team=data"}
        ):
            result = builder._build_job_tags()
        assert result["Team"] == "data"
        assert result["Env"] == "prod"

    def test_env_var_only_no_settings_tags(self):
        builder = _make_builder(tags=None)
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Owner=alice"}
        ):
            result = builder._build_job_tags()
        assert result == {"Owner": "alice"}

    def test_env_var_with_value_containing_equals(self):
        """A VALUE that itself contains '=' should be handled (key=rest of string)."""
        builder = _make_builder(tags=None)
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Url=http://x=1"}
        ):
            result = builder._build_job_tags()
        assert result == {"Url": "http://x=1"}

    def test_empty_env_var_ignored(self):
        builder = _make_builder(tags={"Env": "prod"})
        with patch.dict(os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: ""}):
            result = builder._build_job_tags()
        assert result == {"Env": "prod"}

    def test_trailing_comma_tolerated(self):
        builder = _make_builder(tags=None)
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Env=prod,"}
        ):
            result = builder._build_job_tags()
        assert result == {"Env": "prod"}

    def test_doubled_comma_tolerated(self):
        builder = _make_builder(tags=None)
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Env=prod,,Team=data"}
        ):
            result = builder._build_job_tags()
        assert result == {"Env": "prod", "Team": "data"}

    def test_malformed_pair_without_equals_raises(self):
        """A non-empty pair lacking '=' must raise, not silently vanish."""
        builder = _make_builder(tags=None)
        with patch.dict(os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Env prod"}):
            with pytest.raises(WorkflowError, match="malformed pair"):
                builder._build_job_tags()

    def test_absent_env_var_ignored(self):
        builder = _make_builder(tags={"Env": "prod"})
        env = {
            k: v
            for k, v in os.environ.items()
            if k != SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR
        }
        with patch.dict(os.environ, env, clear=True):
            result = builder._build_job_tags()
        assert result == {"Env": "prod"}


# ---------------------------------------------------------------------------
# Tests for submit() — tags propagation to batch_client.submit_job
# ---------------------------------------------------------------------------


class TestSubmitTagPropagation:
    def _run_submit(self, builder: BatchJobBuilder):
        """Patch build_job_definition and submit_job, then call submit()."""
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
            "jobQueue": "test-queue",
        }
        with patch.object(
            builder,
            "build_job_definition",
            return_value=(_fake_job_def(), "snakejob-test"),
        ):
            return builder.submit(), builder.batch_client.submit_job.call_args

    def test_tags_from_settings_passed_to_submit_job(self):
        builder = _make_builder(tags={"Env": "prod"})
        _, call_args = self._run_submit(builder)
        assert _extract_tags(call_args) == {"Env": "prod"}

    def test_env_var_tags_passed_to_submit_job(self):
        builder = _make_builder(tags=None)
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Team=data"}
        ):
            _, call_args = self._run_submit(builder)
        assert _extract_tags(call_args) == {"Team": "data"}

    def test_merged_tags_passed_to_submit_job(self):
        builder = _make_builder(tags={"Env": "prod"})
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Team=data"}
        ):
            _, call_args = self._run_submit(builder)
        assert _extract_tags(call_args) == {"Env": "prod", "Team": "data"}

    def test_no_tags_key_in_job_params_when_empty(self):
        """When tags is empty, 'tags' should not appear in submit_job call."""
        builder = _make_builder(tags=None)
        env = {
            k: v
            for k, v in os.environ.items()
            if k != SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR
        }
        with patch.dict(os.environ, env, clear=True):
            _, call_args = self._run_submit(builder)
        assert _extract_tags(call_args) is None

    def test_env_var_overrides_settings_in_submit_job(self):
        builder = _make_builder(tags={"Team": "bio"})
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Team=data"}
        ):
            _, call_args = self._run_submit(builder)
        assert _extract_tags(call_args) == {"Team": "data"}

    def test_default_queue_passed_to_submit_job(self):
        builder = _make_builder(tags=None)
        _, call_args = self._run_submit(builder)
        assert call_args.kwargs["jobQueue"] == "test-queue"

    def test_per_rule_queue_override_passed_to_submit_job(self):
        """resources.batch_queue must route the job to the override queue."""
        template = _make_builder(tags=None)
        job = MagicMock()
        job.name = "test_rule"
        job.threads = 1
        job.resources = {"_cores": 1, "mem_mb": 1024, "batch_queue": "override-queue"}
        builder = BatchJobBuilder(
            logger=MagicMock(),
            job=job,
            envvars={},
            container_image="test-image:latest",
            settings=template.settings,
            job_command="snakemake ...",
            batch_client=template.batch_client,
        )
        _, call_args = self._run_submit(builder)
        assert call_args.kwargs["jobQueue"] == "override-queue"


def _extract_tags(call_args) -> dict | None:
    """Extract the 'tags' value from a mock call_args, or None if not present."""
    # call_args is a unittest.mock.call object; kwargs is the preferred accessor
    if call_args is None:
        return None
    kwargs = call_args.kwargs if hasattr(call_args, "kwargs") else call_args[1]
    return kwargs.get("tags")


# ---------------------------------------------------------------------------
# Tests for _get_platform_from_queue — exception handling
# ---------------------------------------------------------------------------


class TestGetPlatformFromQueue:
    def _make_settings(self):
        return SimpleNamespace(
            job_queue="test-queue",
            job_role="arn:aws:iam::123456789:role/test-role",
            tags=None,
            task_timeout=300,
        )

    def _make_job(self):
        job = MagicMock()
        job.name = "test_rule"
        job.threads = 1
        job.resources = {"_cores": 1, "mem_mb": 1024}
        return job

    def _build(self, batch_client):
        return BatchJobBuilder(
            logger=MagicMock(),
            job=self._make_job(),
            envvars={},
            container_image="test-image:latest",
            settings=self._make_settings(),
            job_command="snakemake ...",
            batch_client=batch_client,
        )

    def test_client_error_propagates(self):
        """ClientError from describe_job_queues must not become an EC2 fallback."""
        batch_client = MagicMock()
        batch_client.describe_job_queues.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no perm"}},
            "DescribeJobQueues",
        )
        with pytest.raises(WorkflowError, match="Failed to determine platform"):
            self._build(batch_client)

    def test_unexpected_exception_propagates(self):
        """Unexpected (non-ClientError) exceptions must propagate."""
        batch_client = MagicMock()
        batch_client.describe_job_queues.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            self._build(batch_client)

    def test_empty_queue_response_falls_back_to_ec2(self):
        """The explicit empty-response branch keeps the EC2 fallback."""
        batch_client = MagicMock()
        batch_client.describe_job_queues.return_value = {"jobQueues": []}
        builder = self._build(batch_client)
        assert builder.platform == BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

    def test_empty_compute_environment_response_falls_back_to_ec2(self):
        batch_client = MagicMock()
        batch_client.describe_job_queues.return_value = {
            "jobQueues": [
                {"computeEnvironmentOrder": [{"computeEnvironment": "ce-arn"}]}
            ]
        }
        batch_client.describe_compute_environments.return_value = {
            "computeEnvironments": []
        }
        builder = self._build(batch_client)
        assert builder.platform == BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value

    def test_fargate_compute_environment_detected(self):
        batch_client = MagicMock()
        batch_client.describe_job_queues.return_value = {
            "jobQueues": [
                {"computeEnvironmentOrder": [{"computeEnvironment": "ce-arn"}]}
            ]
        }
        batch_client.describe_compute_environments.return_value = {
            "computeEnvironments": [{"computeResources": {"type": "FARGATE"}}]
        }
        builder = self._build(batch_client)
        assert builder.platform == BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value


# ---------------------------------------------------------------------------
# Tests for _validate_fargate_resources — memory >= requested
# ---------------------------------------------------------------------------


class TestValidateFargateResources:
    def test_picks_smallest_valid_mem_geq_requested(self):
        """vcpu=1, mem=5000 must pick 5120 (smallest valid >= 5000), not 2048."""
        builder = _make_builder(tags=None)
        builder.platform = BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value
        vcpu_str, mem_str = builder._validate_resources("1", "5000")
        assert vcpu_str == "1"
        assert mem_str == "5120"

    def test_picks_exact_mem_when_in_mapping(self):
        builder = _make_builder(tags=None)
        builder.platform = BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value
        vcpu_str, mem_str = builder._validate_resources("1", "4096")
        assert (vcpu_str, mem_str) == ("1", "4096")

    def test_raises_when_request_exceeds_max_for_vcpu(self):
        """vcpu=1 maxes at 8192 MB; requesting 99999 must raise, not shrink."""
        builder = _make_builder(tags=None)
        builder.platform = BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value
        with pytest.raises(WorkflowError, match="exceeds the maximum"):
            builder._validate_resources("1", "99999")

    def test_raises_for_invalid_vcpu(self):
        builder = _make_builder(tags=None)
        builder.platform = BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value
        with pytest.raises(WorkflowError, match="Invalid vCPU"):
            builder._validate_resources("3", "4096")


# ---------------------------------------------------------------------------
# Tests for build_job_definition — tags parity with submit_job
# ---------------------------------------------------------------------------


class TestJobDefinitionTags:
    def test_job_definition_tags_match_submit_tags(self):
        """register_job_definition must get the same validated, env-merged tags."""
        builder = _make_builder(tags={"Env": "prod"})
        builder.batch_client.register_job_definition.return_value = _fake_job_def()
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Team=data"}
        ):
            builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert call_kwargs["tags"] == {"Env": "prod", "Team": "data"}


# ---------------------------------------------------------------------------
# Tests for _validate_ec2_resources
# ---------------------------------------------------------------------------


class TestValidateEc2Resources:
    def test_valid_resources_pass_through(self):
        builder = _make_builder(tags=None)
        assert builder._validate_resources("4", "5000") == ("4", "5000")

    def test_vcpu_below_one_raises(self):
        builder = _make_builder(tags=None)
        with pytest.raises(WorkflowError, match="vCPU must be at least 1"):
            builder._validate_ec2_resources(0, 2048)

    def test_mem_below_1024_raises(self):
        builder = _make_builder(tags=None)
        with pytest.raises(WorkflowError, match="Memory must be at least 1024"):
            builder._validate_ec2_resources(1, 512)


# ---------------------------------------------------------------------------
# Tests for _build_job_tags — validation
# ---------------------------------------------------------------------------


class TestBuildJobTagsValidation:
    def test_empty_key_in_env_var_raises(self):
        builder = _make_builder(tags=None)
        with patch.dict(os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "=value"}):
            with pytest.raises(WorkflowError, match="tag key cannot be empty"):
                builder._build_job_tags()

    def test_empty_key_after_strip_in_env_var_raises(self):
        builder = _make_builder(tags=None)
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "   =value"}
        ):
            with pytest.raises(WorkflowError, match="tag key cannot be empty"):
                builder._build_job_tags()

    def test_empty_key_in_settings_tags_raises(self):
        builder = _make_builder(tags={"": "value"})
        with pytest.raises(WorkflowError, match="tag key cannot be empty"):
            builder._build_job_tags()

    def test_too_many_tags_raises(self):
        too_many = {f"k{i}": str(i) for i in range(51)}
        builder = _make_builder(tags=too_many)
        with pytest.raises(WorkflowError, match="at most 50 tags"):
            builder._build_job_tags()

    def test_exactly_50_tags_ok(self):
        fifty = {f"k{i}": str(i) for i in range(50)}
        builder = _make_builder(tags=fifty)
        result = builder._build_job_tags()
        assert len(result) == 50


# ---------------------------------------------------------------------------
# Tests for error messages — resolved per-job queue
# ---------------------------------------------------------------------------


class TestErrorMessagesUseResolvedQueue:
    """Diagnostics must show the resolved per-job queue (resources.batch_queue
    override), not the profile-wide default from settings."""

    def _make_job_with_queue_override(self):
        job = MagicMock()
        job.name = "test_rule"
        job.threads = 1
        job.resources = {"_cores": 1, "mem_mb": 1024, "batch_queue": "override-queue"}
        return job

    def _make_settings(self):
        return SimpleNamespace(
            job_queue="default-queue",
            job_role="arn:aws:iam::123456789:role/test-role",
            tags=None,
            task_timeout=300,
        )

    def test_platform_detection_error_reports_resolved_queue(self):
        """ClientError diagnostics must name the per-job override queue."""
        batch_client = MagicMock()
        batch_client.describe_job_queues.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no perm"}},
            "DescribeJobQueues",
        )
        with pytest.raises(WorkflowError, match="override-queue"):
            BatchJobBuilder(
                logger=MagicMock(),
                job=self._make_job_with_queue_override(),
                envvars={},
                container_image="test-image:latest",
                settings=self._make_settings(),
                job_command="snakemake ...",
                batch_client=batch_client,
            )

    def test_fargate_rejection_reports_resolved_queue(self):
        """The Fargate fail-fast message must name the per-job override queue."""
        batch_client = MagicMock()
        batch_client.describe_job_queues.return_value = {"jobQueues": []}
        builder = BatchJobBuilder(
            logger=MagicMock(),
            job=self._make_job_with_queue_override(),
            envvars={},
            container_image="test-image:latest",
            settings=self._make_settings(),
            job_command="snakemake ...",
            batch_client=batch_client,
        )
        builder.platform = BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value
        with pytest.raises(WorkflowError, match="override-queue"):
            builder.build_job_definition()


# ---------------------------------------------------------------------------
# Tests for build_job_definition — shared_memory_size_mb
# ---------------------------------------------------------------------------


class TestSharedMemorySize:
    def _build_with_shm(self, shm_value):
        builder = _make_builder(tags=None)
        builder.job.resources = dict(
            builder.job.resources, shared_memory_size_mb=shm_value
        )
        builder.batch_client.register_job_definition.return_value = _fake_job_def()
        return builder

    def test_shm_size_sets_linux_parameters(self):
        builder = self._build_with_shm(4096)
        builder.build_job_definition()
        props = builder.batch_client.register_job_definition.call_args.kwargs[
            "containerProperties"
        ]
        assert props["linuxParameters"] == {"sharedMemorySize": 4096}

    def test_shm_size_accepts_string_values(self):
        """Snakemake resources may arrive as strings; ints must still come out."""
        builder = self._build_with_shm("2048")
        builder.build_job_definition()
        props = builder.batch_client.register_job_definition.call_args.kwargs[
            "containerProperties"
        ]
        assert props["linuxParameters"] == {"sharedMemorySize": 2048}

    def test_unset_shm_size_omits_linux_parameters(self):
        builder = _make_builder(tags=None)
        builder.batch_client.register_job_definition.return_value = _fake_job_def()
        builder.build_job_definition()
        props = builder.batch_client.register_job_definition.call_args.kwargs[
            "containerProperties"
        ]
        assert "linuxParameters" not in props

    def test_non_numeric_shm_size_raises_workflow_error(self):
        builder = self._build_with_shm("4g")
        with pytest.raises(WorkflowError, match="shared_memory_size_mb"):
            builder.build_job_definition()

    def test_negative_shm_size_raises_workflow_error(self):
        builder = self._build_with_shm(-64)
        with pytest.raises(WorkflowError, match="positive"):
            builder.build_job_definition()


# ---------------------------------------------------------------------------
# Tests for build_job_definition — Fargate rejection
# ---------------------------------------------------------------------------


class TestBuildJobDefinitionFargateRejection:
    def test_fargate_platform_raises_workflow_error(self):
        """build_job_definition must reject Fargate until properties are wired."""
        builder = _make_builder(tags=None)
        builder.platform = BATCH_JOB_PLATFORM_CAPABILITIES.FARGATE.value
        with pytest.raises(WorkflowError, match="Fargate"):
            builder.build_job_definition()
        # No registration should have been attempted.
        builder.batch_client.register_job_definition.assert_not_called()

    def test_ec2_platform_does_not_raise(self):
        builder = _make_builder(tags=None)
        # _make_builder leaves platform == EC2 (empty queue response branch).
        assert builder.platform == BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value
        builder.batch_client.register_job_definition.return_value = {
            "jobDefinitionName": "snakejob-def-test",
            "revision": 1,
        }
        job_def, job_name = builder.build_job_definition()
        assert job_def["jobDefinitionName"] == "snakejob-def-test"
        assert job_name.startswith("snakejob-test_rule-")


# ---------------------------------------------------------------------------
# Tests for task_timeout behaviour (conditional timeout, AWS 60s minimum)
# ---------------------------------------------------------------------------


def _make_timeout_builder(task_timeout) -> BatchJobBuilder:
    """Return a BatchJobBuilder with the given task_timeout.

    Mocks short-circuit platform detection to EC2 (empty queue response).
    """
    settings = SimpleNamespace(
        job_queue="test-queue",
        job_role="arn:aws:iam::123456789:role/test-role",
        tags=None,
        task_timeout=task_timeout,
    )
    batch_client = MagicMock()
    batch_client.describe_job_queues.return_value = {"jobQueues": []}
    batch_client.register_job_definition.return_value = _fake_job_def()

    job = MagicMock()
    job.name = "test_rule"
    job.threads = 1
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
        builder = _make_timeout_builder(None)
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert "timeout" not in call_kwargs

    def test_set_timeout_includes_timeout_in_register_kwargs(self):
        """Set task_timeout: register_job_definition must receive the timeout dict."""
        builder = _make_timeout_builder(3600)
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 3600}

    def test_minimum_valid_timeout_60_passes(self):
        """60 seconds is the AWS minimum; it must not raise."""
        builder = _make_timeout_builder(60)
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 60}

    def test_timeout_below_60_raises_workflow_error(self):
        """task_timeout < 60 must raise WorkflowError before calling the API."""
        builder = _make_timeout_builder(59)
        with pytest.raises(WorkflowError, match="60"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()

    def test_timeout_of_1_raises_workflow_error(self):
        """Any value below 60 must be rejected."""
        builder = _make_timeout_builder(1)
        with pytest.raises(WorkflowError, match="60"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()

    def test_timeout_of_0_raises_workflow_error(self):
        """Zero is below the AWS minimum; must raise WorkflowError."""
        builder = _make_timeout_builder(0)
        with pytest.raises(WorkflowError, match="60"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()

    def test_timeout_negative_raises_workflow_error(self):
        """Negative values are below the AWS minimum; must raise WorkflowError."""
        builder = _make_timeout_builder(-1)
        with pytest.raises(WorkflowError, match="60"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for build_job_definition — per-rule aws_batch_task_timeout resource
# ---------------------------------------------------------------------------


def _make_builder_with_rule_timeout(
    setting_timeout=None, resource_timeout=None
) -> BatchJobBuilder:
    """Return a BatchJobBuilder for per-rule timeout tests.

    setting_timeout  — value for settings.task_timeout (None = no global timeout).
    resource_timeout — value for job.resources["aws_batch_task_timeout"]
                       (None = key absent from resources dict).
    """
    resources: dict = {"_cores": 1, "mem_mb": 1024}
    if resource_timeout is not None:
        resources["aws_batch_task_timeout"] = resource_timeout

    settings = SimpleNamespace(
        job_queue="test-queue",
        job_role="arn:aws:iam::123456789:role/test-role",
        tags=None,
        task_timeout=setting_timeout,
    )
    batch_client = MagicMock()
    batch_client.describe_job_queues.return_value = {"jobQueues": []}
    batch_client.register_job_definition.return_value = _fake_job_def()

    job = MagicMock()
    job.name = "test_rule"
    job.threads = 1
    job.resources = resources

    return BatchJobBuilder(
        logger=MagicMock(),
        job=job,
        envvars={},
        container_image="test-image:latest",
        settings=settings,
        job_command="snakemake ...",
        batch_client=batch_client,
    )


class TestPerRuleTaskTimeout:
    def test_resource_overrides_setting(self):
        """aws_batch_task_timeout resource takes precedence over the global setting."""
        builder = _make_builder_with_rule_timeout(
            setting_timeout=300, resource_timeout=14400
        )
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 14400}

    def test_resource_alone_no_setting(self):
        """Per-rule resource works when settings.task_timeout is None."""
        builder = _make_builder_with_rule_timeout(
            setting_timeout=None, resource_timeout=7200
        )
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 7200}

    def test_fallback_to_setting_when_resource_absent(self):
        """When resource is absent, the global setting is used."""
        builder = _make_builder_with_rule_timeout(
            setting_timeout=3600, resource_timeout=None
        )
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 3600}

    def test_both_absent_omits_timeout(self):
        """When neither resource nor setting is set, timeout is omitted."""
        builder = _make_builder_with_rule_timeout(
            setting_timeout=None, resource_timeout=None
        )
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert "timeout" not in call_kwargs

    def test_resource_below_60_raises_workflow_error(self):
        """Per-rule timeout < 60 must raise WorkflowError."""
        builder = _make_builder_with_rule_timeout(
            setting_timeout=None, resource_timeout=30
        )
        with pytest.raises(WorkflowError, match="60"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()

    def test_non_numeric_resource_raises_workflow_error(self):
        """Non-numeric aws_batch_task_timeout (e.g. '4h') must raise WorkflowError."""
        builder = _make_builder_with_rule_timeout(
            setting_timeout=None, resource_timeout="4h"
        )
        with pytest.raises(WorkflowError, match="aws_batch_task_timeout"):
            builder.build_job_definition()
        builder.batch_client.register_job_definition.assert_not_called()

    def test_resource_exactly_60_passes(self):
        """Exactly 60 seconds via resource is the AWS minimum and must not raise."""
        builder = _make_builder_with_rule_timeout(
            setting_timeout=None, resource_timeout=60
        )
        builder.build_job_definition()
        call_kwargs = builder.batch_client.register_job_definition.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 60}
