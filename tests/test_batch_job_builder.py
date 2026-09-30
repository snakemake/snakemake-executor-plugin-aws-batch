"""Unit tests for BatchJobBuilder.

Covers tag propagation, platform detection error handling, Fargate resource
validation, and Fargate rejection in build_job_definition. All tests run with
mocked AWS clients — no AWS credentials required.
"""

import os
import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from snakemake.common.tbdstring import TBDString
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_aws_batch.batch_job_builder import (
    SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR,
    BatchJobBuilder,
    _sanitize_job_name,
    MAX_RULE_NAME_LENGTH,
    TRUNCATION_SUFFIX,
    AWS_BATCH_MAX_NAME_LENGTH,
)
from snakemake_interface_executor_plugins.jobs import GroupJobExecutorInterface
from snakemake_executor_plugin_aws_batch.constant import (
    BATCH_JOB_PLATFORM_CAPABILITIES,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_builder(tags=None, name="test_rule") -> BatchJobBuilder:
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
    job.name = name
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

    def test_propagate_tags_set_when_tags_present(self):
        """propagateTags=True must be included in submit_job kwargs when tags exist."""
        builder = _make_builder(tags={"Env": "prod"})
        _, call_args = self._run_submit(builder)
        assert _extract_propagate_tags(call_args) is True

    def test_propagate_tags_set_when_only_env_var_tags(self):
        """propagateTags=True must appear even when tags come only from the env var."""
        builder = _make_builder(tags=None)
        with patch.dict(
            os.environ, {SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR: "Team=data"}
        ):
            _, call_args = self._run_submit(builder)
        assert _extract_propagate_tags(call_args) is True

    def test_propagate_tags_absent_when_no_tags(self):
        """propagateTags must not appear in submit_job kwargs when there are no tags."""
        builder = _make_builder(tags=None)
        env = {
            k: v
            for k, v in os.environ.items()
            if k != SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR
        }
        with patch.dict(os.environ, env, clear=True):
            _, call_args = self._run_submit(builder)
        # Assert key absence directly: an explicit propagateTags=None would also
        # satisfy `.get(...) is None` but is not what we want to send to the SDK.
        assert "propagateTags" not in call_args.kwargs


def _extract_tags(call_args) -> dict | None:
    """Extract the 'tags' value from a mock call_args, or None if not present."""
    # call_args is a unittest.mock.call object; kwargs is the preferred accessor
    if call_args is None:
        return None
    kwargs = call_args.kwargs if hasattr(call_args, "kwargs") else call_args[1]
    return kwargs.get("tags")


def _extract_propagate_tags(call_args) -> bool | None:
    """Extract the 'propagateTags' value from a mock call_args, or None if absent."""
    if call_args is None:
        return None
    kwargs = call_args.kwargs if hasattr(call_args, "kwargs") else call_args[1]
    return kwargs.get("propagateTags")


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
        # Platform is resolved lazily, so the error surfaces on first access.
        builder = self._build(batch_client)
        with pytest.raises(WorkflowError, match="Failed to determine platform"):
            _ = builder.platform

    def test_unexpected_exception_propagates(self):
        """Unexpected (non-ClientError) exceptions must propagate."""
        batch_client = MagicMock()
        batch_client.describe_job_queues.side_effect = RuntimeError("boom")
        builder = self._build(batch_client)
        with pytest.raises(RuntimeError, match="boom"):
            _ = builder.platform

    def test_platform_not_resolved_at_construction(self):
        """Constructing a builder must not query the queue — resolution is lazy."""
        batch_client = MagicMock()
        batch_client.describe_job_queues.return_value = {"jobQueues": []}
        self._build(batch_client)
        batch_client.describe_job_queues.assert_not_called()

    def test_platform_cached_after_first_access(self):
        """The queue is queried once; subsequent accesses use the cached value."""
        batch_client = MagicMock()
        batch_client.describe_job_queues.return_value = {"jobQueues": []}
        builder = self._build(batch_client)
        _ = builder.platform
        _ = builder.platform
        batch_client.describe_job_queues.assert_called_once()

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
        builder = BatchJobBuilder(
            logger=MagicMock(),
            job=self._make_job_with_queue_override(),
            envvars={},
            container_image="test-image:latest",
            settings=self._make_settings(),
            job_command="snakemake ...",
            batch_client=batch_client,
        )
        # Platform is resolved lazily, so the error surfaces on first access.
        with pytest.raises(WorkflowError, match="override-queue"):
            _ = builder.platform

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
# Tests for build_job_definition — log_group
# ---------------------------------------------------------------------------


class TestLogGroup:
    def _build(self, **settings):
        builder = _make_builder(tags=None)
        for key, value in settings.items():
            setattr(builder.settings, key, value)
        builder.batch_client.register_job_definition.return_value = _fake_job_def()
        builder.build_job_definition()
        return builder.batch_client.register_job_definition.call_args.kwargs[
            "containerProperties"
        ]

    def test_log_group_sets_awslogs_configuration(self):
        props = self._build(log_group="/snakemake/my-project", region="us-east-1")
        assert props["logConfiguration"] == {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": "/snakemake/my-project",
                "awslogs-region": "us-east-1",
            },
        }

    def test_log_group_without_region_omits_awslogs_region(self):
        props = self._build(log_group="/snakemake/my-project")
        assert props["logConfiguration"]["options"] == {
            "awslogs-group": "/snakemake/my-project"
        }

    def test_unset_log_group_omits_log_configuration(self):
        props = self._build(log_group=None, region="us-east-1")
        assert "logConfiguration" not in props


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


# ---------------------------------------------------------------------------
# Tests for submit() — scheduling priority
# ---------------------------------------------------------------------------


def _make_builder_with_priority(setting_priority=None, resource_priority=None):
    """Return a BatchJobBuilder configured for scheduling-priority tests.

    setting_priority  — value for settings.scheduling_priority (None = unset).
    resource_priority — value for job.resources["aws_batch_scheduling_priority"]
                        (None = key absent from resources dict).
    """
    resources = {"_cores": 1, "mem_mb": 1024}
    if resource_priority is not None:
        resources["aws_batch_scheduling_priority"] = resource_priority

    settings = SimpleNamespace(
        job_queue="test-queue",
        job_role="arn:aws:iam::123456789:role/test-role",
        tags=None,
        scheduling_priority=setting_priority,
    )

    batch_client = MagicMock()
    batch_client.describe_job_queues.return_value = {"jobQueues": []}

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


def _run_submit_priority(builder):
    """Patch build_job_definition and call submit(); return submit_job call_args."""
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
        builder.submit()
    return builder.batch_client.submit_job.call_args


class TestSubmitSchedulingPriority:
    def test_unset_omits_kwarg(self):
        """When neither setting nor resource is set, schedulingPriorityOverride
        must be absent from the submit_job call."""
        builder = _make_builder_with_priority(
            setting_priority=None, resource_priority=None
        )
        call_args = _run_submit_priority(builder)
        assert "schedulingPriorityOverride" not in call_args.kwargs

    def test_setting_only_present_with_value(self):
        """A workflow-level setting is forwarded when no per-rule override exists."""
        builder = _make_builder_with_priority(
            setting_priority=50, resource_priority=None
        )
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["schedulingPriorityOverride"] == 50

    def test_resource_overrides_setting(self):
        """Per-rule aws_batch_scheduling_priority takes precedence over the setting."""
        builder = _make_builder_with_priority(
            setting_priority=10, resource_priority=100
        )
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["schedulingPriorityOverride"] == 100

    def test_resource_alone_works(self):
        """Per-rule resource without any workflow-level setting is applied."""
        builder = _make_builder_with_priority(
            setting_priority=None, resource_priority=75
        )
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["schedulingPriorityOverride"] == 75

    def test_non_numeric_resource_raises_workflow_error(self):
        """A non-numeric aws_batch_scheduling_priority resource raises WorkflowError
        and does not call submit_job."""
        builder = _make_builder_with_priority(
            setting_priority=None, resource_priority="high"
        )
        with patch.object(
            builder,
            "build_job_definition",
            return_value=(_fake_job_def(), "snakejob-test"),
        ):
            with pytest.raises(WorkflowError, match="aws_batch_scheduling_priority"):
                builder.submit()
        builder.batch_client.submit_job.assert_not_called()

    def test_zero_priority_is_forwarded(self):
        """Priority of 0 is a valid fair-share value and must be included in the call
        (regression pin against a future ``if priority:`` refactor)."""
        builder = _make_builder_with_priority(
            setting_priority=None, resource_priority=0
        )
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["schedulingPriorityOverride"] == 0

    def test_non_numeric_setting_raises_workflow_error(self):
        """A non-numeric --aws-batch-scheduling-priority setting raises WorkflowError
        and does not call submit_job.  Error message must mention the setting, not
        the resource, because the bad value came from the CLI flag."""
        builder = _make_builder_with_priority(
            setting_priority="high", resource_priority=None
        )
        with patch.object(
            builder,
            "build_job_definition",
            return_value=(_fake_job_def(), "snakejob-test"),
        ):
            with pytest.raises(WorkflowError, match="--aws-batch-scheduling-priority"):
                builder.submit()
        builder.batch_client.submit_job.assert_not_called()

    def test_out_of_range_resource_raises_workflow_error(self):
        """A priority outside the AWS Batch [0, 9999] range raises WorkflowError
        before calling submit_job."""
        builder = _make_builder_with_priority(
            setting_priority=None, resource_priority=10000
        )
        with patch.object(
            builder,
            "build_job_definition",
            return_value=(_fake_job_def(), "snakejob-test"),
        ):
            with pytest.raises(WorkflowError, match="range"):
                builder.submit()
        builder.batch_client.submit_job.assert_not_called()

    def test_out_of_range_setting_raises_workflow_error(self):
        """A workflow-level scheduling priority outside [0, 9999] raises WorkflowError."""  # noqa: E501
        builder = _make_builder_with_priority(
            setting_priority=10000, resource_priority=None
        )
        with patch.object(
            builder,
            "build_job_definition",
            return_value=(_fake_job_def(), "snakejob-test"),
        ):
            with pytest.raises(WorkflowError, match="range"):
                builder.submit()
        builder.batch_client.submit_job.assert_not_called()

    def test_negative_priority_raises_workflow_error(self):
        """A negative priority is below the AWS Batch minimum (0) and must raise
        WorkflowError before calling submit_job (lower-bound regression pin)."""
        builder = _make_builder_with_priority(
            setting_priority=None, resource_priority=-1
        )
        with patch.object(
            builder,
            "build_job_definition",
            return_value=(_fake_job_def(), "snakejob-test"),
        ):
            with pytest.raises(WorkflowError, match="range"):
                builder.submit()
        builder.batch_client.submit_job.assert_not_called()

    def test_max_valid_priority_is_forwarded(self):
        """Priority of 9999 (AWS maximum) must be accepted and forwarded."""
        builder = _make_builder_with_priority(
            setting_priority=None, resource_priority=9999
        )
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["schedulingPriorityOverride"] == 9999


# ---------------------------------------------------------------------------
# Tests for spot attempts (retryStrategy)
# ---------------------------------------------------------------------------


def _expected_retry_strategy(attempts: int) -> dict:
    return {
        "attempts": attempts,
        "evaluateOnExit": [
            {"onStatusReason": "Host EC2*", "action": "RETRY"},
            {"onReason": "*", "action": "EXIT"},
        ],
    }


def _make_builder_with_spot_attempts(setting_attempts=None, resource_attempts=None):
    """Return a BatchJobBuilder for spot-attempts tests (None = unset/absent)."""
    builder = _make_builder_with_priority()
    builder.settings.spot_attempts = setting_attempts
    if resource_attempts is not None:
        builder.job.resources["aws_batch_spot_attempts"] = resource_attempts
    return builder


class TestSubmitSpotAttempts:
    def test_unset_omits_retry_strategy(self):
        builder = _make_builder_with_spot_attempts()
        call_args = _run_submit_priority(builder)
        assert "retryStrategy" not in call_args.kwargs

    def test_setting_retries_only_host_termination(self):
        builder = _make_builder_with_spot_attempts(setting_attempts=3)
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["retryStrategy"] == _expected_retry_strategy(3)

    def test_resource_overrides_setting(self):
        builder = _make_builder_with_spot_attempts(
            setting_attempts=3, resource_attempts=1
        )
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["retryStrategy"] == _expected_retry_strategy(1)

    def test_resource_alone_works(self):
        builder = _make_builder_with_spot_attempts(resource_attempts="5")
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["retryStrategy"] == _expected_retry_strategy(5)

    @pytest.mark.parametrize("attempts", [1, 10])
    def test_aws_limits_are_accepted(self, attempts):
        builder = _make_builder_with_spot_attempts(setting_attempts=attempts)
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["retryStrategy"]["attempts"] == attempts

    @pytest.mark.parametrize(
        "setting, resource, match",
        [
            (0, None, r"--aws-batch-spot-attempts setting 0: must be in range"),
            (11, None, r"--aws-batch-spot-attempts setting 11: must be in range"),
            ("many", None, r"--aws-batch-spot-attempts setting 'many'"),
            (3, 0, r"aws_batch_spot_attempts resource 0: must be in range"),
            (None, "many", r"aws_batch_spot_attempts resource 'many'"),
        ],
    )
    def test_invalid_values_raise_before_submit(self, setting, resource, match):
        builder = _make_builder_with_spot_attempts(
            setting_attempts=setting, resource_attempts=resource
        )
        with patch.object(
            builder,
            "build_job_definition",
            return_value=(_fake_job_def(), "snakejob-test"),
        ):
            with pytest.raises(WorkflowError, match=match):
                builder.submit()
        builder.batch_client.submit_job.assert_not_called()

    def test_invalid_resource_raises_before_registering_a_definition(self):
        """An invalid resource must not leave a registered job definition behind."""
        builder = _make_builder(tags=None)
        builder.job.resources = dict(builder.job.resources, aws_batch_spot_attempts=11)
        with pytest.raises(WorkflowError, match="aws_batch_spot_attempts resource"):
            builder.submit()
        builder.batch_client.register_job_definition.assert_not_called()
        builder.batch_client.submit_job.assert_not_called()

    def test_not_set_on_the_registered_job_definition(self):
        """SubmitJob carries the strategy; the registered definition does not."""
        builder = _make_builder(tags=None)
        builder.settings.spot_attempts = 3
        builder.batch_client.register_job_definition.return_value = _fake_job_def()
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        register_kwargs = builder.batch_client.register_job_definition.call_args
        assert "retryStrategy" not in register_kwargs.kwargs
        submit_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert submit_kwargs["retryStrategy"] == _expected_retry_strategy(3)


def _group_of(*member_resources: dict) -> MagicMock:
    """A group job whose members have the given resources. The group's own
    resources are what Snakemake would give it: parallel members' ints summed."""
    group = MagicMock(spec=GroupJobExecutorInterface)
    group.name = "test_group"
    group.threads = 1
    group.jobs = [MagicMock(resources=dict(r)) for r in member_resources]
    group.resources = {
        "_cores": 1,
        "mem_mb": 1024,
        "aws_batch_spot_attempts": sum(
            r.get("aws_batch_spot_attempts", 0)
            for r in member_resources
            if not isinstance(r.get("aws_batch_spot_attempts"), str)
        ),
    }
    return group


class TestGroupSpotAttempts:
    """A group job gets the smallest of its members' spot attempts."""

    @pytest.mark.parametrize(
        "setting, members, expected",
        [
            # Summed, the group's own resources would give 2 (a Batch retry).
            (None, [{"aws_batch_spot_attempts": 1}] * 2, 1),
            # Summed, 12 would be out of range and stop the workflow.
            (None, [{"aws_batch_spot_attempts": 3}] * 4, 3),
            # A member that opts out opts the group out.
            (3, [{"aws_batch_spot_attempts": 1}, {}], 1),
            # Members without the resource fall back to the setting.
            (4, [{}, {}], 4),
            (4, [{"aws_batch_spot_attempts": 6}, {}], 4),
        ],
    )
    def test_smallest_member_value(self, setting, members, expected):
        builder = _make_builder_with_spot_attempts(setting_attempts=setting)
        builder.job = _group_of(*members)
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["retryStrategy"] == _expected_retry_strategy(expected)

    @pytest.mark.parametrize(
        "setting, expected",
        [
            # A member Snakemake cannot evaluate yet falls back to the setting.
            (2, _expected_retry_strategy(2)),
            # With no setting, only the other member's value counts.
            (None, _expected_retry_strategy(5)),
        ],
    )
    def test_member_not_yet_evaluated_is_unset(self, setting, expected):
        builder = _make_builder_with_spot_attempts(setting_attempts=setting)
        builder.job = _group_of(
            {"aws_batch_spot_attempts": 5}, {"aws_batch_spot_attempts": TBDString()}
        )
        call_args = _run_submit_priority(builder)
        assert call_args.kwargs["retryStrategy"] == expected

    def test_unset_everywhere_omits_retry_strategy(self):
        builder = _make_builder_with_spot_attempts()
        builder.job = _group_of({}, {})
        call_args = _run_submit_priority(builder)
        assert "retryStrategy" not in call_args.kwargs

    def test_invalid_member_value_raises(self):
        builder = _make_builder_with_spot_attempts()
        builder.job = _group_of({"aws_batch_spot_attempts": 0}, {})
        with pytest.raises(WorkflowError, match="aws_batch_spot_attempts resource 0"):
            _run_submit_priority(builder)


class TestSpotAttemptsSetting:
    """--aws-batch-spot-attempts is validated when the settings are built."""

    def test_unset_is_none(self):
        from snakemake_executor_plugin_aws_batch import ExecutorSettings

        assert ExecutorSettings().spot_attempts is None

    def test_valid_value_is_normalized_to_int(self):
        from snakemake_executor_plugin_aws_batch import ExecutorSettings

        assert ExecutorSettings(spot_attempts="3").spot_attempts == 3

    @pytest.mark.parametrize(
        "attempts, match",
        [
            (0, r"setting 0: must be in range \[1, 10\]"),
            (11, r"setting 11: must be in range \[1, 10\]"),
            ("many", r"setting 'many': must be an integer"),
        ],
    )
    def test_invalid_value_raises_at_startup(self, attempts, match):
        from snakemake_executor_plugin_aws_batch import ExecutorSettings

        with pytest.raises(WorkflowError, match=match):
            ExecutorSettings(spot_attempts=attempts)


# ---------------------------------------------------------------------------
# Tests for the per-rule aws_batch_container_image resource (Executor.run_job)
# ---------------------------------------------------------------------------


def _run_job_capture_container_image(
    resources: dict, global_image: str = "global-image:latest"
) -> str:
    """Call Executor.run_job with the given job.resources (BatchJobBuilder mocked)
    and return the container_image it passed to BatchJobBuilder."""
    from snakemake_executor_plugin_aws_batch import Executor

    executor = object.__new__(Executor)
    executor.logger = MagicMock()
    executor.batch_client = MagicMock()
    executor.settings = SimpleNamespace(
        job_queue="test-queue", job_role="test-role", tags=None, task_timeout=None
    )
    executor.container_image = global_image
    executor.envvars = MagicMock(return_value={})
    executor.format_job_exec = MagicMock(return_value="snakemake ...")
    executor.report_job_submission = MagicMock()

    job = MagicMock()
    job.resources = resources

    with patch("snakemake_executor_plugin_aws_batch.BatchJobBuilder") as mock_cls:
        instance = mock_cls.return_value
        instance.submit.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
            "jobQueue": "test-queue",
        }
        instance.job_queue = "test-queue"
        executor.run_job(job)
    return mock_cls.call_args.kwargs["container_image"]


class TestRunJobContainerImage:
    """Executor.run_job resolves the per-rule aws_batch_container_image resource."""

    def test_rule_resource_overrides_global_image(self):
        """The aws_batch_container_image resource must override the global image."""
        image = _run_job_capture_container_image(
            {"aws_batch_container_image": "rule-image:v2"}
        )
        assert image == "rule-image:v2"

    def test_falls_back_to_global_when_resource_absent(self):
        """With no per-rule resource, run_job must use the global container image."""
        image = _run_job_capture_container_image({})
        assert image == "global-image:latest"


# ---------------------------------------------------------------------------
# Tests for pre-existing job definitions (--aws-batch-job-definition)
# ---------------------------------------------------------------------------


def _make_builder_with_preexisting(
    job_definition: str = "my-job-def",
    resources: dict | None = None,
    job_role: str | None = None,
    envvars: dict | None = None,
    threads: int = 2,
) -> BatchJobBuilder:
    """Return a BatchJobBuilder configured with a pre-existing job definition."""
    if resources is None:
        resources = {"_cores": 2, "mem_mb": 2048}
    settings = SimpleNamespace(
        job_queue="test-queue",
        job_role=job_role,
        job_definition=job_definition,
        tags=None,
        task_timeout=300,
    )
    batch_client = MagicMock()
    batch_client.describe_job_queues.return_value = {"jobQueues": []}
    job = MagicMock()
    job.name = "test_rule"
    job.threads = threads
    job.resources = resources
    return BatchJobBuilder(
        logger=MagicMock(),
        job=job,
        envvars=envvars or {},
        container_image="test-image:latest",
        settings=settings,
        job_command="snakemake --jobs 1",
        batch_client=batch_client,
    )


class TestPreExistingJobDefinition:
    """Pre-existing job definition mode: skip register, use containerOverrides."""

    # --- submit uses supplied definition, no register call ---

    def test_submit_uses_preexisting_definition_no_register(self):
        """submit() must use the pre-existing definition and skip register."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        builder.batch_client.register_job_definition.assert_not_called()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert call_kwargs["jobDefinition"] == "my-job-def"

    def test_submit_with_arn_definition(self):
        """An ARN-form pre-existing definition must be passed through unchanged."""
        arn = "arn:aws:batch:us-east-1:123456789012:job-definition/my-def:5"
        builder = _make_builder_with_preexisting(job_definition=arn)
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert call_kwargs["jobDefinition"] == arn

    # --- containerOverrides carries command, env, vcpu, mem ---

    def test_container_overrides_has_command(self):
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        overrides = call_kwargs["containerOverrides"]
        assert overrides["command"] == ["/bin/bash", "-c", "snakemake --jobs 1"]

    def test_container_overrides_has_vcpu_and_mem(self):
        builder = _make_builder_with_preexisting(
            resources={"_cores": 4, "mem_mb": 8192}, threads=4
        )
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        overrides = builder.batch_client.submit_job.call_args.kwargs[
            "containerOverrides"
        ]
        req_by_type = {r["type"]: r["value"] for r in overrides["resourceRequirements"]}
        assert req_by_type["VCPU"] == "4"
        assert req_by_type["MEMORY"] == "8192"

    def test_container_overrides_has_environment(self):
        builder = _make_builder_with_preexisting(
            job_definition="my-job-def",
            envvars={"MY_VAR": "hello", "ANOTHER": "world"},
        )
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        overrides = builder.batch_client.submit_job.call_args.kwargs[
            "containerOverrides"
        ]
        env_map = {e["name"]: e["value"] for e in overrides.get("environment", [])}
        assert env_map["MY_VAR"] == "hello"
        assert env_map["ANOTHER"] == "world"

    # --- tags flow through _build_job_tags() into submit_job ---

    def test_tags_forwarded_in_preexisting_mode(self):
        """Settings tags must reach submit_job via the same _build_job_tags() path."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.settings.tags = {"Env": "prod", "Project": "fgumi"}
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert call_kwargs["tags"] == {"Env": "prod", "Project": "fgumi"}
        # propagateTags must be mirrored here just like the dynamic path so the
        # tags reach the underlying ECS task (see submit()).
        assert call_kwargs["propagateTags"] is True

    def test_propagate_tags_absent_in_preexisting_mode_when_no_tags(self):
        """propagateTags must not appear when there are no tags in pre-existing mode."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.settings.tags = None
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        env = {
            k: v
            for k, v in os.environ.items()
            if k != SNAKEMAKE_AWS_BATCH_JOB_TAGS_ENV_VAR
        }
        with patch.dict(os.environ, env, clear=True):
            builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert "propagateTags" not in call_kwargs

    def test_container_overrides_no_gpu_when_zero(self):
        """GPU must NOT appear in resourceRequirements when gpu == 0."""
        builder = _make_builder_with_preexisting(
            resources={"_cores": 2, "mem_mb": 2048, "_gpus": 0}
        )
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        overrides = builder.batch_client.submit_job.call_args.kwargs[
            "containerOverrides"
        ]
        types = [r["type"] for r in overrides["resourceRequirements"]]
        assert "GPU" not in types

    def test_container_overrides_includes_gpu_when_set(self):
        """GPU must appear in resourceRequirements when gpu > 0."""
        builder = _make_builder_with_preexisting(
            resources={"_cores": 4, "mem_mb": 4096, "_gpus": 2}, threads=4
        )
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        overrides = builder.batch_client.submit_job.call_args.kwargs[
            "containerOverrides"
        ]
        req_by_type = {r["type"]: r["value"] for r in overrides["resourceRequirements"]}
        assert req_by_type["GPU"] == "2"

    # --- per-rule aws_batch_job_definition resource override ---

    def test_per_rule_resource_overrides_setting(self):
        """resources.aws_batch_job_definition overrides the settings value."""
        builder = _make_builder_with_preexisting(
            job_definition="default-def",
            resources={
                "_cores": 2,
                "mem_mb": 2048,
                "aws_batch_job_definition": "per-rule-def",
            },
        )
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert call_kwargs["jobDefinition"] == "per-rule-def"
        builder.batch_client.register_job_definition.assert_not_called()

    # --- incompatible setting combinations raise WorkflowError ---

    def test_job_role_plus_job_definition_raises(self):
        """job_role + job_definition must raise WorkflowError immediately."""
        builder = _make_builder_with_preexisting(
            job_definition="my-job-def",
            job_role="arn:aws:iam::123456789:role/my-role",
        )
        with pytest.raises(WorkflowError, match="job_role"):
            builder.submit()
        builder.batch_client.register_job_definition.assert_not_called()
        builder.batch_client.submit_job.assert_not_called()

    def test_log_group_plus_job_definition_raises(self):
        """log_group + job_definition must raise WorkflowError."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.settings.log_group = "/snakemake/my-project"
        with pytest.raises(WorkflowError, match="log_group"):
            builder.submit()
        builder.batch_client.register_job_definition.assert_not_called()
        builder.batch_client.submit_job.assert_not_called()

    def test_log_group_plus_per_rule_job_definition_raises(self):
        """log_group + a per-rule aws_batch_job_definition must raise too."""
        builder = _make_builder_with_preexisting(
            job_definition=None,
            resources={
                "_cores": 2,
                "mem_mb": 2048,
                "aws_batch_job_definition": "per-rule-def",
            },
        )
        builder.settings.log_group = "/snakemake/my-project"
        with pytest.raises(WorkflowError, match="aws_batch_job_definition"):
            builder.submit()
        builder.batch_client.register_job_definition.assert_not_called()
        builder.batch_client.submit_job.assert_not_called()

    def test_shared_memory_size_mb_plus_job_definition_raises(self):
        """shared_memory_size_mb + job_definition must raise WorkflowError."""
        builder = _make_builder_with_preexisting(
            job_definition="my-job-def",
            resources={"_cores": 2, "mem_mb": 2048, "shared_memory_size_mb": 4096},
        )
        with pytest.raises(WorkflowError, match="shared_memory_size_mb"):
            builder.submit()
        builder.batch_client.register_job_definition.assert_not_called()
        builder.batch_client.submit_job.assert_not_called()

    def test_shared_memory_size_mb_zero_plus_job_definition_raises(self):
        """An explicit shared_memory_size_mb=0 must also raise — the resource is
        meaningless in pre-existing mode, and the dynamic path rejects 0 too."""
        builder = _make_builder_with_preexisting(
            job_definition="my-job-def",
            resources={"_cores": 2, "mem_mb": 2048, "shared_memory_size_mb": 0},
        )
        with pytest.raises(WorkflowError, match="shared_memory_size_mb"):
            builder.submit()
        builder.batch_client.submit_job.assert_not_called()

    # --- no platform discovery in pre-existing mode (smaller IAM surface) ---

    def test_no_queue_platform_discovery_in_preexisting_mode(self):
        """Pre-existing mode must not query the queue — that would force
        DescribeJobQueues/DescribeComputeEnvironments IAM the mode aims to avoid."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        builder.batch_client.describe_job_queues.assert_not_called()
        builder.batch_client.describe_compute_environments.assert_not_called()

    # --- task timeout is mirrored into the pre-existing path ---

    def test_task_timeout_forwarded_in_preexisting_mode(self):
        """The dynamic path bakes timeout into the definition; pre-existing mode
        must forward it as SubmitJob's top-level timeout field instead."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.settings.task_timeout = 600
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 600}

    def test_per_rule_task_timeout_overrides_setting_in_preexisting_mode(self):
        """The aws_batch_task_timeout resource takes precedence over the setting."""
        builder = _make_builder_with_preexisting(
            job_definition="my-job-def",
            resources={
                "_cores": 2,
                "mem_mb": 2048,
                "aws_batch_task_timeout": 900,
            },
        )
        builder.settings.task_timeout = 600
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert call_kwargs["timeout"] == {"attemptDurationSeconds": 900}

    def test_task_timeout_omitted_when_unset_in_preexisting_mode(self):
        """No timeout field when neither resource nor setting is configured."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.settings.task_timeout = None
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert "timeout" not in call_kwargs

    # --- no deregister for pre-existing definitions ---

    def test_submitted_job_aux_marks_preexisting(self):
        """Jobs submitted with a pre-existing definition must carry the
        _preexisting_job_definition marker so deregister can skip them."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        result = builder.submit()
        assert result.get("_preexisting_job_definition") is True

    # --- scheduling priority is mirrored into the pre-existing path ---

    def test_scheduling_priority_forwarded_in_preexisting_mode(self):
        """schedulingPriorityOverride must be mirrored from the dynamic path."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.settings.scheduling_priority = 500
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert call_kwargs["schedulingPriorityOverride"] == 500

    def test_scheduling_priority_omitted_when_unset_in_preexisting_mode(self):
        """schedulingPriorityOverride must be absent when no priority is set."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert "schedulingPriorityOverride" not in call_kwargs

    # --- spot attempts are mirrored into the pre-existing path ---

    def test_spot_attempts_forwarded_in_preexisting_mode(self):
        """retryStrategy must be mirrored from the dynamic path."""
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.settings.spot_attempts = 4
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert call_kwargs["retryStrategy"] == _expected_retry_strategy(4)

    def test_spot_attempts_resource_forwarded_in_preexisting_mode(self):
        builder = _make_builder_with_preexisting(
            job_definition="my-job-def",
            resources={"_cores": 2, "mem_mb": 2048, "aws_batch_spot_attempts": 2},
        )
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert call_kwargs["retryStrategy"] == _expected_retry_strategy(2)

    def test_spot_attempts_omitted_when_unset_in_preexisting_mode(self):
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        call_kwargs = builder.batch_client.submit_job.call_args.kwargs
        assert "retryStrategy" not in call_kwargs

    # --- default path unchanged when no pre-existing definition configured ---

    def test_default_path_no_preexisting_def_still_registers(self):
        """When job_definition is absent from settings, the dynamic path runs."""
        builder = _make_builder(tags=None)  # settings has no job_definition attr
        builder.batch_client.register_job_definition.return_value = _fake_job_def()
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        builder.submit()
        builder.batch_client.register_job_definition.assert_called_once()

    def test_settings_job_definition_none_uses_dynamic_path(self):
        """Explicit job_definition=None must fall through to the dynamic path."""
        settings = SimpleNamespace(
            job_queue="test-queue",
            job_role="arn:aws:iam::123456789:role/role",
            job_definition=None,
            tags=None,
            task_timeout=300,
        )
        batch_client = MagicMock()
        batch_client.describe_job_queues.return_value = {"jobQueues": []}
        batch_client.register_job_definition.return_value = _fake_job_def()
        batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        job = MagicMock()
        job.name = "test_rule"
        job.threads = 1
        job.resources = {"_cores": 1, "mem_mb": 1024}
        builder = BatchJobBuilder(
            logger=MagicMock(),
            job=job,
            envvars={},
            container_image="test-image:latest",
            settings=settings,
            job_command="snakemake ...",
            batch_client=batch_client,
        )
        builder.submit()
        batch_client.register_job_definition.assert_called_once()


class TestDeregisterSkipsPreexisting:
    """Executor._deregister_job must skip jobs that used a pre-existing definition."""

    def _make_executor(self):
        """Return a minimal Executor-like object with a real _deregister_job method."""
        from snakemake_executor_plugin_aws_batch import Executor

        executor = object.__new__(Executor)
        executor.logger = MagicMock()
        executor.batch_client = MagicMock()
        return executor

    def test_deregister_skipped_for_preexisting(self):
        """_deregister_job must skip deregistration when _preexisting flag set."""
        executor = self._make_executor()
        job = MagicMock()
        job.aux = {
            "job_definition_arn": "arn:aws:batch:::job-definition/my-def:1",
            "_preexisting_job_definition": True,
        }
        executor._deregister_job(job)
        executor.batch_client.deregister_job_definition.assert_not_called()

    def test_deregister_skip_logged_at_debug(self):
        """Skipping a pre-existing definition must be logged at debug so operators
        can tell why nothing was deregistered."""
        executor = self._make_executor()
        job = MagicMock()
        job.aux = {
            "job_definition_arn": "arn:aws:batch:::job-definition/my-def:1",
            "_preexisting_job_definition": True,
        }
        executor._deregister_job(job)
        executor.logger.debug.assert_called_once()
        assert (
            "skipping deregistration" in executor.logger.debug.call_args.args[0].lower()
        )

    def test_deregister_runs_for_dynamic_definition(self):
        """_deregister_job must call deregister_job_definition for dynamic defs."""
        executor = self._make_executor()
        job = MagicMock()
        job.aux = {
            "job_definition_arn": "arn:aws:batch:::job-definition/snakejob-def:1",
        }
        executor._deregister_job(job)
        executor.batch_client.deregister_job_definition.assert_called_once_with(
            jobDefinition="arn:aws:batch:::job-definition/snakejob-def:1"
        )

    def test_marker_survives_real_flow_end_to_end(self):
        """The _preexisting_job_definition marker must survive the full submit →
        SubmittedJobInfo → _interpret_job_status aux-write → _deregister_job chain.

        This locks the marker-survival property against a future refactor that
        reassigns job.aux wholesale: if the marker is ever lost between submit() and
        _deregister_job(), this test will catch it.
        """
        from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo

        # 1. Call submit() in pre-existing mode with a mocked batch client.
        builder = _make_builder_with_preexisting(job_definition="my-job-def")
        builder.batch_client.submit_job.return_value = {
            "jobName": "snakejob-test",
            "jobId": "abc-123",
        }
        job_info = builder.submit()

        # 2. Build SubmittedJobInfo exactly as run_job does.
        mock_job = MagicMock()
        submitted = SubmittedJobInfo(
            job=mock_job, external_jobid=job_info["jobId"], aux=dict(job_info)
        )

        # 3. Simulate _interpret_job_status's in-place aux write (the real code
        #    writes individual keys into the existing dict, not a reassignment).
        submitted.aux[
            "job_definition_arn"
        ] = "arn:aws:batch:us-east-1:123456789012:job-definition/my-job-def:1"

        # 4. Call _deregister_job and assert deregistration was skipped.
        executor = self._make_executor()
        executor._deregister_job(submitted)
        executor.batch_client.deregister_job_definition.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for the default-mode job-role requirement in build_job_definition
# ---------------------------------------------------------------------------


class TestBuildJobDefinitionJobRole:
    """A job role is mandatory in the default (register-per-job) path.

    Enforced here — not at global preflight — so a workflow driving pre-existing
    definitions purely through the per-rule ``aws_batch_job_definition`` resource
    (which is dispatched before ``build_job_definition``) can run without a
    global job role.
    """

    def test_default_mode_requires_job_role(self):
        builder = _make_builder()
        builder.settings.job_role = None  # default mode, role forgotten
        with pytest.raises(WorkflowError, match="requires a job role"):
            builder.build_job_definition()

    def test_default_mode_with_job_role_builds_definition(self):
        builder = _make_builder()  # helper supplies a valid job_role
        builder.job.resources = {"_cores": 1, "mem_mb": 2048}
        job_def, job_name = builder.build_job_definition()
        assert job_name.startswith("snakejob-test_rule-")
        builder.batch_client.register_job_definition.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for _sanitize_job_name
# ---------------------------------------------------------------------------

_UUID_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


class TestSanitizeJobName:
    """Tests for the _sanitize_job_name helper function."""

    def test_simple_name_unchanged(self):
        """Simple alphanumeric names should pass through unchanged."""
        assert _sanitize_job_name("align") == "align"
        assert _sanitize_job_name("my_rule") == "my_rule"
        assert _sanitize_job_name("rule-name") == "rule-name"

    def test_invalid_characters_replaced(self):
        """Characters not allowed in AWS Batch names should be replaced."""
        # Dots replaced with underscores
        assert _sanitize_job_name("rule.name") == "rule_name"
        # Colons replaced
        assert _sanitize_job_name("rule:name") == "rule_name"
        # Multiple invalid chars
        assert _sanitize_job_name("a.b:c/d") == "a_b_c_d"

    def test_non_ascii_characters_replaced(self):
        """Non-ASCII letters (valid in Snakemake rule names) should be replaced."""
        assert _sanitize_job_name("règle") == "r_gle"
        assert _sanitize_job_name("ñame_1") == "ame_1"
        assert _sanitize_job_name("数据") == "job"

    def test_multiple_underscores_collapsed(self):
        """Multiple consecutive underscores should be collapsed to one."""
        assert _sanitize_job_name("rule__name") == "rule_name"
        assert _sanitize_job_name("a...b") == "a_b"

    def test_leading_trailing_stripped(self):
        """Leading and trailing underscores/hyphens should be stripped."""
        assert _sanitize_job_name("_rule_") == "rule"
        assert _sanitize_job_name("-rule-") == "rule"
        assert _sanitize_job_name("__rule__") == "rule"

    def test_truncation_at_max_length(self):
        """Names exceeding max length should be truncated with suffix."""
        long_name = "a" * 100
        result = _sanitize_job_name(long_name)
        expected = (
            "a" * (MAX_RULE_NAME_LENGTH - len(TRUNCATION_SUFFIX)) + TRUNCATION_SUFFIX
        )
        assert result == expected
        assert len(result) == MAX_RULE_NAME_LENGTH

    @pytest.mark.parametrize("separator", ["_", "-", "."])
    def test_truncation_strips_trailing_separator(self, separator):
        """Truncation should not leave a trailing separator before the suffix."""
        # Place the separator at the last position kept by truncation
        stem = "a" * (MAX_RULE_NAME_LENGTH - len(TRUNCATION_SUFFIX) - 1)
        name = stem + separator + "b" * 10
        result = _sanitize_job_name(name)
        assert result == stem + TRUNCATION_SUFFIX
        assert len(result) <= MAX_RULE_NAME_LENGTH

    def test_empty_name_returns_job(self):
        """Empty or all-invalid names should return 'job' as fallback."""
        assert _sanitize_job_name("") == "job"
        assert _sanitize_job_name("...") == "job"
        assert _sanitize_job_name("___") == "job"

    def test_group_job_name_format(self):
        """Test sanitization of typical GroupJob name format."""
        # GroupJob.name format: "{groupid}_{rule1}_{rule2}_..."
        group_name = "alignment_group_align_sort_index_mark_duplicates"
        result = _sanitize_job_name(group_name)
        assert result == group_name  # Should be unchanged if valid

    def test_group_job_with_ellipsis(self):
        """GroupJob names with '...' (from truncation) should be sanitized."""
        # Snakemake adds '...' when >5 rules in group
        name = "group_rule1_rule2_rule3_rule4_rule5_..."
        assert _sanitize_job_name(name) == "group_rule1_rule2_rule3_rule4_rule5"

    def test_custom_max_length(self):
        """Custom max_length parameter should be respected."""
        name = "abcdefghij"
        result = _sanitize_job_name(name, max_length=5)
        # 5 - 2 (suffix) = 3 chars + suffix
        assert result == "abc" + TRUNCATION_SUFFIX
        assert len(result) == 5

    @pytest.mark.parametrize("max_length", [-1, 0, 1, len(TRUNCATION_SUFFIX)])
    def test_max_length_too_small_raises(self, max_length):
        """A max_length that cannot fit any name plus the suffix is rejected."""
        with pytest.raises(ValueError, match="max_length must be greater than"):
            _sanitize_job_name("abcdefghij", max_length=max_length)


# ---------------------------------------------------------------------------
# Tests for _build_job_names integration
# ---------------------------------------------------------------------------


class TestBuildJobNamesIntegration:
    """Tests that build_job_definition and preexisting path use _sanitize_job_name."""

    def _make_builder_with_name(self, job_name: str) -> BatchJobBuilder:
        """Create a builder with a custom job name for testing sanitization."""
        builder = _make_builder(name=job_name)
        builder.batch_client.register_job_definition.return_value = _fake_job_def()
        # Force EC2 platform to avoid Fargate rejection
        builder.platform = BATCH_JOB_PLATFORM_CAPABILITIES.EC2.value
        return builder

    @staticmethod
    def _registered_job_def_name(builder: BatchJobBuilder) -> str:
        call_args = builder.batch_client.register_job_definition.call_args
        return call_args.kwargs["jobDefinitionName"]

    def test_build_job_definition_sanitizes_dotted_name(self):
        """build_job_definition should sanitize job names with invalid characters."""
        builder = self._make_builder_with_name("rule.with.dots")
        job_def, job_name = builder.build_job_definition()

        assert re.fullmatch(f"snakejob-rule_with_dots-{_UUID_PATTERN}", job_name)
        assert re.fullmatch(
            f"snakejob-def-rule_with_dots-{_UUID_PATTERN}",
            self._registered_job_def_name(builder),
        )
        builder.logger.debug.assert_called_once()

    def test_build_job_definition_valid_name_not_logged(self):
        """A name that needs no sanitization should not log a sanitization message."""
        builder = self._make_builder_with_name("valid_rule")
        builder.build_job_definition()
        builder.logger.debug.assert_not_called()

    def test_build_job_definition_sanitizes_long_name(self):
        """build_job_definition should truncate overly long job names."""
        long_name = "a" * 100
        builder = self._make_builder_with_name(long_name)
        job_def, job_name = builder.build_job_definition()

        # Should be truncated with suffix
        expected_stem = (
            "a" * (MAX_RULE_NAME_LENGTH - len(TRUNCATION_SUFFIX)) + TRUNCATION_SUFFIX
        )
        assert re.fullmatch(f"snakejob-{expected_stem}-{_UUID_PATTERN}", job_name)
        # The job definition name is the binding constraint (longer prefix)
        job_def_name = self._registered_job_def_name(builder)
        assert re.fullmatch(
            f"snakejob-def-{expected_stem}-{_UUID_PATTERN}", job_def_name
        )
        assert len(job_def_name) == AWS_BATCH_MAX_NAME_LENGTH

    def test_build_job_definition_exact_max_length_passes(self):
        """A rule name at exactly MAX_RULE_NAME_LENGTH should not be truncated."""
        exact_name = "a" * MAX_RULE_NAME_LENGTH
        builder = self._make_builder_with_name(exact_name)
        job_def, job_name = builder.build_job_definition()

        # Should NOT be truncated — no suffix appended
        assert re.fullmatch(f"snakejob-{exact_name}-{_UUID_PATTERN}", job_name)
        # The job definition name uses the full AWS Batch limit, so
        # MAX_RULE_NAME_LENGTH is neither too long nor needlessly short.
        job_def_name = self._registered_job_def_name(builder)
        assert re.fullmatch(f"snakejob-def-{exact_name}-{_UUID_PATTERN}", job_def_name)
        assert len(job_def_name) == AWS_BATCH_MAX_NAME_LENGTH

    def test_preexisting_path_sanitizes_dotted_name(self):
        """_submit_with_preexisting_definition should sanitize job names."""
        builder = self._make_builder_with_name("rule.with.dots")
        # Remove job_role to avoid validation error in preexisting path
        builder.settings.job_role = None
        builder.batch_client.submit_job.return_value = {
            "jobId": "test-job-id",
            "jobName": "test-job-name",
        }

        builder._submit_with_preexisting_definition(
            "arn:aws:batch:us-east-1:123456789:job-definition/my-def:1"
        )

        # Check the jobName passed to submit_job
        call_args = builder.batch_client.submit_job.call_args
        submitted_job_name = call_args.kwargs["jobName"]
        assert re.fullmatch(
            f"snakejob-rule_with_dots-{_UUID_PATTERN}", submitted_job_name
        )
        builder.batch_client.register_job_definition.assert_not_called()
