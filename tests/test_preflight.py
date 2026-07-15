"""Unit tests for the executor's startup preflight validation.

Covers ``_preflight_validate``, ``_queue_problems``, and ``_validate_job_role``:
the best-effort checks run in ``__post_init__`` that fail fast on a definitively
misconfigured job queue / compute environment / job role, but degrade to a
no-op on uncertain state (a transient API error or a missing describe/iam
permission).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_aws_batch import Executor


def _executor(**settings) -> Executor:
    """Build a bare Executor (bypassing __post_init__) with mocks in place.

    Defaults to a valid ``job_role`` — the realistic default (register-per-job)
    configuration. Tests exercising pre-existing-definition mode pass
    ``job_role=None`` explicitly.
    """
    base = {
        "region": "us-east-1",
        "job_queue": "arn:q",
        "job_role": "arn:aws:iam::1:role/default",
    }
    base.update(settings)
    ex = Executor.__new__(Executor)
    ex.logger = MagicMock()
    ex.settings = SimpleNamespace(**base)
    ex.batch_client = MagicMock()
    return ex


def _with_queue(ex: Executor, queue, compute_envs=None) -> Executor:
    """Stub describe_job_queues / describe_compute_environments on the client."""
    ex.batch_client.describe_job_queues.return_value = {
        "jobQueues": [queue] if queue else []
    }
    ex.batch_client.describe_compute_environments.return_value = {
        "computeEnvironments": compute_envs or []
    }
    return ex


class TestQueueProblems:
    def test_no_queue_configured_returns_none(self):
        ex = _executor(job_queue=None)
        assert ex._queue_problems() is None

    def test_queue_not_found_reported(self):
        ex = _with_queue(_executor(), None)
        assert ex._queue_problems() == ["job queue not found"]

    def test_healthy_queue_has_no_problems(self):
        ex = _with_queue(
            _executor(),
            {"state": "ENABLED", "status": "VALID", "computeEnvironmentOrder": []},
        )
        assert ex._queue_problems() == []

    def test_disabled_queue_reported(self):
        ex = _with_queue(
            _executor(), {"state": "DISABLED", "computeEnvironmentOrder": []}
        )
        assert any("DISABLED" in p for p in ex._queue_problems())

    def test_fatal_queue_status_reported(self):
        ex = _with_queue(
            _executor(),
            {"state": "ENABLED", "status": "INVALID", "computeEnvironmentOrder": []},
        )
        assert any("INVALID" in p for p in ex._queue_problems())

    def test_transient_queue_status_not_reported(self):
        # A queue mid-update (UPDATING) is recoverable and must NOT be flagged.
        ex = _with_queue(
            _executor(),
            {"state": "ENABLED", "status": "UPDATING", "computeEnvironmentOrder": []},
        )
        assert ex._queue_problems() == []

    def test_compute_env_maxvcpus_zero_reported(self):
        ex = _with_queue(
            _executor(),
            {
                "state": "ENABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
            compute_envs=[
                {
                    "computeEnvironmentName": "ce1",
                    "state": "ENABLED",
                    "status": "VALID",
                    "computeResources": {"maxvCpus": 0},
                }
            ],
        )
        assert any("maxvCpus=0" in p for p in ex._queue_problems())

    def test_compute_env_disabled_state_reported(self):
        ex = _with_queue(
            _executor(),
            {
                "state": "ENABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
            compute_envs=[
                {
                    "computeEnvironmentName": "ce1",
                    "state": "DISABLED",
                    "status": "VALID",
                }
            ],
        )
        assert any("ce1 is DISABLED" in p for p in ex._queue_problems())

    def test_compute_env_fatal_status_reported(self):
        ex = _with_queue(
            _executor(),
            {
                "state": "ENABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
            compute_envs=[
                {
                    "computeEnvironmentName": "ce1",
                    "state": "ENABLED",
                    "status": "INVALID",
                }
            ],
        )
        assert any("ce1 status is INVALID" in p for p in ex._queue_problems())

    def test_mixed_healthy_and_unhealthy_compute_envs_not_reported(self):
        # AWS Batch falls back across compute environments, so one bad CE next
        # to a healthy one does not block jobs — preflight must not flag it.
        ex = _with_queue(
            _executor(),
            {
                "state": "ENABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [
                    {"computeEnvironment": "bad"},
                    {"computeEnvironment": "good"},
                ],
            },
            compute_envs=[
                {
                    "computeEnvironmentName": "bad",
                    "state": "DISABLED",
                    "status": "VALID",
                },
                {
                    "computeEnvironmentName": "good",
                    "state": "ENABLED",
                    "status": "VALID",
                    "computeResources": {"maxvCpus": 16},
                },
            ],
        )
        assert ex._queue_problems() == []

    def test_all_unhealthy_compute_envs_reported(self):
        # Only when EVERY compute environment is unusable is the queue blocked.
        ex = _with_queue(
            _executor(),
            {
                "state": "ENABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [
                    {"computeEnvironment": "a"},
                    {"computeEnvironment": "b"},
                ],
            },
            compute_envs=[
                {"computeEnvironmentName": "a", "state": "DISABLED", "status": "VALID"},
                {
                    "computeEnvironmentName": "b",
                    "state": "ENABLED",
                    "status": "VALID",
                    "computeResources": {"maxvCpus": 0},
                },
            ],
        )
        problems = ex._queue_problems()
        assert problems
        assert any("a is DISABLED" in p for p in problems)
        assert any("maxvCpus=0" in p for p in problems)

    def test_compute_env_describe_failure_preserves_queue_problems(self):
        # A failure describing the compute environment(s) must NOT discard a
        # confirmed queue-level problem (e.g. a DISABLED queue) collected first.
        ex = _with_queue(
            _executor(),
            {
                "state": "DISABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
        )
        ex.batch_client.describe_compute_environments.side_effect = Exception(
            "AccessDenied"
        )
        assert any("DISABLED" in p for p in ex._queue_problems())

    def test_compute_env_describe_failure_healthy_queue_returns_empty(self):
        # If the queue is healthy and only the CE describe fails, degrade to an
        # empty problem list (not None) — nothing confirmed-bad was found.
        ex = _with_queue(
            _executor(),
            {
                "state": "ENABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
        )
        ex.batch_client.describe_compute_environments.side_effect = Exception(
            "AccessDenied"
        )
        assert ex._queue_problems() == []

    def test_api_error_returns_none(self):
        ex = _executor()
        ex.batch_client.describe_job_queues.side_effect = Exception("throttled")
        assert ex._queue_problems() is None


class TestPreflightValidate:
    def test_healthy_passes_and_checks_role(self):
        ex = _with_queue(
            _executor(),
            {"state": "ENABLED", "status": "VALID", "computeEnvironmentOrder": []},
        )
        ex._validate_job_role = MagicMock()
        ex._preflight_validate()  # must not raise
        ex._validate_job_role.assert_called_once()

    def test_disabled_queue_raises(self):
        ex = _with_queue(
            _executor(), {"state": "DISABLED", "computeEnvironmentOrder": []}
        )
        with pytest.raises(WorkflowError, match="DISABLED"):
            ex._preflight_validate()

    def test_maxvcpus_zero_raises(self):
        ex = _with_queue(
            _executor(),
            {
                "state": "ENABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
            compute_envs=[
                {
                    "computeEnvironmentName": "ce1",
                    "state": "ENABLED",
                    "status": "VALID",
                    "computeResources": {"maxvCpus": 0},
                }
            ],
        )
        with pytest.raises(WorkflowError, match="maxvCpus=0"):
            ex._preflight_validate()

    def test_disabled_queue_raises_even_when_compute_env_describe_fails(self):
        # Regression: a missing batch:DescribeComputeEnvironments permission must
        # not mask a definitively DISABLED queue.
        ex = _with_queue(
            _executor(),
            {
                "state": "DISABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
        )
        ex.batch_client.describe_compute_environments.side_effect = Exception(
            "AccessDenied"
        )
        ex._validate_job_role = MagicMock()
        with pytest.raises(WorkflowError, match="DISABLED"):
            ex._preflight_validate()

    def test_uncertain_state_does_not_raise(self):
        # An API error -> _queue_problems returns None -> preflight proceeds.
        ex = _executor()
        ex.batch_client.describe_job_queues.side_effect = Exception("throttled")
        ex._validate_job_role = MagicMock()
        ex._preflight_validate()  # must not raise
        ex._validate_job_role.assert_called_once()

    def test_no_queue_configured_does_not_raise(self):
        ex = _executor(job_queue=None)
        ex._validate_job_role = MagicMock()
        ex._preflight_validate()  # must not raise
        ex._validate_job_role.assert_called_once()


class TestPreflightJobDefinitionJobRoleCombo:
    """`job_definition` + `job_role` must fail fast, before any queue/role call.

    A pre-existing job definition bakes in its own role, so combining it with
    `--aws-batch-job-role` can never work. The per-rule resource is caught later
    in BatchJobBuilder; this covers the setting-level combo at preflight.
    """

    def test_both_set_raises_before_queue_or_role_checks(self):
        ex = _executor(
            job_definition="my-job-def",
            job_role="arn:aws:iam::1:role/my-role",
        )
        ex._queue_problems = MagicMock()
        ex._validate_job_role = MagicMock()
        with pytest.raises(WorkflowError) as excinfo:
            ex._preflight_validate()
        msg = str(excinfo.value)
        assert "--aws-batch-job-role" in msg
        assert "--aws-batch-job-definition" in msg
        # Fail fast: neither the queue describe nor the iam:GetRole check runs.
        ex._queue_problems.assert_not_called()
        ex._validate_job_role.assert_not_called()

    def test_job_definition_only_does_not_raise(self):
        # Pre-existing-definition mode: job_role omitted (it carries its own).
        ex = _with_queue(
            _executor(job_definition="my-job-def", job_role=None),
            {"state": "ENABLED", "status": "VALID", "computeEnvironmentOrder": []},
        )
        ex._validate_job_role = MagicMock()
        ex._preflight_validate()  # must not raise
        ex._validate_job_role.assert_called_once()

    def test_no_job_role_without_job_definition_raises(self):
        # Default (register-per-job) mode requires a job role; the setting is no
        # longer `required` at the framework layer, so preflight enforces it.
        ex = _executor(job_role=None)
        ex._queue_problems = MagicMock()
        with pytest.raises(WorkflowError, match="requires a job role"):
            ex._preflight_validate()
        # Fail fast: the config error surfaces before any queue describe.
        ex._queue_problems.assert_not_called()

    def test_job_role_only_preserves_existing_behaviour(self):
        ex = _with_queue(
            _executor(job_role="arn:aws:iam::1:role/my-role"),
            {"state": "ENABLED", "status": "VALID", "computeEnvironmentOrder": []},
        )
        ex._validate_job_role = MagicMock()
        ex._preflight_validate()  # must not raise
        ex._validate_job_role.assert_called_once()


class TestValidateJobRole:
    def _iam(self, **get_role_kwargs) -> MagicMock:
        return MagicMock(get_role=MagicMock(**get_role_kwargs))

    def _client_error(self, code: str) -> ClientError:
        return ClientError({"Error": {"Code": code}}, "GetRole")

    def test_missing_role_raises(self):
        ex = _executor(job_role="arn:aws:iam::1:role/missing")
        iam = self._iam(side_effect=self._client_error("NoSuchEntity"))
        with patch("boto3.client", return_value=iam):
            with pytest.raises(WorkflowError, match="does not exist"):
                ex._validate_job_role()

    def test_access_denied_degrades(self):
        ex = _executor(job_role="arn:aws:iam::1:role/maybe")
        iam = self._iam(side_effect=self._client_error("AccessDenied"))
        with patch("boto3.client", return_value=iam):
            ex._validate_job_role()  # must not raise

    def test_existing_role_passes_with_bare_name(self):
        # A pathed ARN must resolve to the final segment for GetRole.
        ex = _executor(job_role="arn:aws:iam::1:role/team/path/good")
        iam = self._iam(return_value={"Role": {}})
        with patch("boto3.client", return_value=iam):
            ex._validate_job_role()
        iam.get_role.assert_called_once_with(RoleName="good")

    def test_no_role_configured_is_noop(self):
        ex = _executor(job_role=None)
        with patch("boto3.client") as mocked_client:
            ex._validate_job_role()  # returns before creating any client
            mocked_client.assert_not_called()
