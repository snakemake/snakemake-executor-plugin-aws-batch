"""Unit tests for the executor's preflight validation.

Covers ``_preflight_validate`` (startup job-role check), ``_validate_queue`` /
``_queue_problems`` (per-effective-queue health, validated at submission time and
cached), and ``_validate_job_role``: the best-effort checks that fail fast on a
definitively misconfigured job queue / compute environment / job role, but
degrade to a no-op on uncertain state (a transient API error or a missing
describe/iam permission).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_aws_batch import Executor


def _executor(**settings) -> Executor:
    """Build a bare Executor (bypassing __post_init__) with mocks in place."""
    base = {"region": "us-east-1", "job_queue": "arn:q", "job_role": None}
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


def _healthy_ce(name: str = "ce1") -> dict:
    """A usable compute environment (ENABLED/VALID with capacity)."""
    return {
        "computeEnvironmentName": name,
        "state": "ENABLED",
        "status": "VALID",
        "computeResources": {"maxvCpus": 16},
    }


def _healthy_queue() -> dict:
    """An ENABLED/VALID queue attached to one usable compute environment."""
    return {
        "state": "ENABLED",
        "status": "VALID",
        "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
    }


class TestQueueProblems:
    def test_no_queue_configured_returns_none(self):
        ex = _executor(job_queue=None)
        assert ex._queue_problems() is None

    def test_queue_not_found_reported(self):
        ex = _with_queue(_executor(), None)
        assert ex._queue_problems() == ["job queue not found"]

    def test_healthy_queue_has_no_problems(self):
        ex = _with_queue(_executor(), _healthy_queue(), compute_envs=[_healthy_ce()])
        assert ex._queue_problems() == []

    def test_queue_without_compute_environment_reported(self):
        # An ENABLED/VALID queue with an empty computeEnvironmentOrder has no
        # compute attached, so jobs would sit RUNNABLE forever.
        ex = _with_queue(
            _executor(),
            {"state": "ENABLED", "status": "VALID", "computeEnvironmentOrder": []},
        )
        assert ex._queue_problems() == ["job queue has no compute environment attached"]

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
            {
                "state": "ENABLED",
                "status": "UPDATING",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
            compute_envs=[_healthy_ce()],
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

    def test_all_referenced_compute_envs_missing_reported(self):
        # describe_compute_environments SUCCEEDS but returns none of the
        # referenced CEs (all deleted): a confirmed-unusable queue, not an
        # uncertain state, so it must be flagged (distinct from the describe-
        # failure/exception path, which degrades).
        ex = _with_queue(
            _executor(),
            {
                "state": "ENABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
            compute_envs=[],
        )
        problems = ex._queue_problems()
        assert problems
        assert any("not found" in p and "ce1" in p for p in problems)


class TestPreflightValidate:
    def test_healthy_passes_and_checks_role(self):
        ex = _with_queue(_executor(), _healthy_queue(), compute_envs=[_healthy_ce()])
        ex._validate_job_role = MagicMock()
        ex._preflight_validate()  # must not raise
        ex._validate_job_role.assert_called_once()

    def test_does_not_validate_global_queue_at_startup(self):
        # The global queue may be unused (jobs can pick a per-rule batch_queue),
        # so startup must NOT describe it or reject the workflow on its health —
        # even when it is definitively broken. Queue health is checked per
        # effective queue at submission time (see TestValidateQueue).
        ex = _with_queue(
            _executor(), {"state": "DISABLED", "computeEnvironmentOrder": []}
        )
        ex._validate_job_role = MagicMock()
        ex._preflight_validate()  # must not raise on the broken global queue
        ex.batch_client.describe_job_queues.assert_not_called()
        ex._validate_job_role.assert_called_once()

    def test_no_queue_configured_does_not_raise(self):
        ex = _executor(job_queue=None)
        ex._validate_job_role = MagicMock()
        ex._preflight_validate()  # must not raise
        ex._validate_job_role.assert_called_once()


class TestValidateQueue:
    def _ex(self):
        ex = _executor()
        ex._queue_validation_cache = {}
        return ex

    def test_healthy_queue_passes(self):
        ex = _with_queue(self._ex(), _healthy_queue(), compute_envs=[_healthy_ce()])
        ex._validate_queue("arn:q")  # must not raise

    def test_none_queue_is_noop(self):
        ex = self._ex()
        ex._validate_queue(None)
        ex._validate_queue("")
        ex.batch_client.describe_job_queues.assert_not_called()

    def test_queue_without_compute_environment_raises(self):
        ex = _with_queue(
            self._ex(),
            {"state": "ENABLED", "status": "VALID", "computeEnvironmentOrder": []},
        )
        with pytest.raises(WorkflowError, match="no compute environment attached"):
            ex._validate_queue("arn:q")

    def test_disabled_queue_raises(self):
        ex = _with_queue(
            self._ex(), {"state": "DISABLED", "computeEnvironmentOrder": []}
        )
        with pytest.raises(WorkflowError, match="DISABLED"):
            ex._validate_queue("arn:q")

    def test_error_message_names_the_effective_queue(self):
        ex = _with_queue(
            self._ex(), {"state": "DISABLED", "computeEnvironmentOrder": []}
        )
        with pytest.raises(WorkflowError, match="arn:per-rule-queue"):
            ex._validate_queue("arn:per-rule-queue")

    def test_maxvcpus_zero_raises(self):
        ex = _with_queue(
            self._ex(),
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
            ex._validate_queue("arn:q")

    def test_uncertain_state_does_not_raise(self):
        # A describe error -> _queue_problems returns None -> no rejection.
        ex = self._ex()
        ex.batch_client.describe_job_queues.side_effect = Exception("throttled")
        ex._validate_queue("arn:q")  # must not raise

    def test_compute_env_describe_failure_does_not_mask_disabled_queue(self):
        # Regression: a missing batch:DescribeComputeEnvironments permission must
        # not mask a definitively DISABLED queue — the queue-level problem is
        # still surfaced through the public _validate_queue entry point.
        ex = _with_queue(
            self._ex(),
            {
                "state": "DISABLED",
                "status": "VALID",
                "computeEnvironmentOrder": [{"computeEnvironment": "ce1"}],
            },
        )
        ex.batch_client.describe_compute_environments.side_effect = Exception(
            "AccessDenied"
        )
        with pytest.raises(WorkflowError, match="DISABLED"):
            ex._validate_queue("arn:q")

    def test_result_is_cached_per_queue(self):
        # A queue is described once; a second validation of the same queue reuses
        # the cached result instead of re-describing it.
        ex = _with_queue(self._ex(), _healthy_queue(), compute_envs=[_healthy_ce()])
        ex._validate_queue("arn:q")
        ex._validate_queue("arn:q")
        assert ex.batch_client.describe_job_queues.call_count == 1

    def test_uncertain_result_is_cached_and_not_reprobed(self):
        # Contract: an uncertain (None) result is cached too, so a queue is
        # described at most once per run. A transient/permission describe failure
        # therefore disables the check for that queue for the rest of the run —
        # even if a later describe would have found it broken. This is deliberate
        # (a describe failure is almost always a persistent missing permission;
        # re-describing every job would only spam warnings).
        ex = self._ex()
        ex.batch_client.describe_job_queues.side_effect = Exception("throttled")
        ex._validate_queue("arn:q")  # first probe fails -> None cached
        # A subsequent describe would report a DISABLED queue, but it is never
        # called: the cached None short-circuits, so no raise and no re-describe.
        ex.batch_client.describe_job_queues.side_effect = None
        ex.batch_client.describe_job_queues.return_value = {
            "jobQueues": [{"state": "DISABLED", "computeEnvironmentOrder": []}]
        }
        ex._validate_queue("arn:q")  # must not raise, must not re-describe
        assert ex.batch_client.describe_job_queues.call_count == 1

    def test_distinct_queues_each_validated(self):
        # A healthy per-rule queue is not blocked by an unhealthy global queue:
        # each distinct effective queue is validated on its own.
        ex = self._ex()

        def describe(jobQueues):
            arn = jobQueues[0]
            if arn == "arn:bad":
                return {"jobQueues": [{"state": "DISABLED"}]}
            return {"jobQueues": [_healthy_queue()]}

        ex.batch_client.describe_job_queues.side_effect = describe
        ex.batch_client.describe_compute_environments.return_value = {
            "computeEnvironments": [_healthy_ce()]
        }
        ex._validate_queue("arn:good")  # healthy per-rule queue: must not raise
        with pytest.raises(WorkflowError, match="DISABLED"):
            ex._validate_queue("arn:bad")


class TestRunJobValidatesEffectiveQueue:
    """run_job validates the queue a job actually targets, before submitting."""

    def _executor_for_run_job(self):
        ex = _executor()
        ex.container_image = "img:1"
        ex._queue_validation_cache = {}
        ex.envvars = MagicMock(return_value={})
        ex.format_job_exec = MagicMock(return_value="cmd")
        ex.report_job_submission = MagicMock()
        ex._validate_queue = MagicMock()
        return ex

    def _builder(self, job_queue="arn:per-rule-queue", preexisting=False):
        builder = MagicMock()
        builder.job_queue = job_queue
        builder.uses_preexisting_job_definition = preexisting
        builder.submit.return_value = {"jobName": "n", "jobId": "j"}
        return builder

    def _job(self, batch_queue=None):
        resources = {}
        if batch_queue is not None:
            resources["batch_queue"] = batch_queue
        return SimpleNamespace(resources=resources, name="rule1")

    def test_validates_effective_queue_before_submit(self):
        ex = self._executor_for_run_job()
        builder = self._builder(job_queue="arn:per-rule-queue")
        # Attach both calls to one parent so their relative order is observable.
        parent = MagicMock()
        parent.attach_mock(ex._validate_queue, "validate")
        parent.attach_mock(builder.submit, "submit")
        with patch(
            "snakemake_executor_plugin_aws_batch.BatchJobBuilder",
            return_value=builder,
        ):
            ex.run_job(self._job(batch_queue="arn:per-rule-queue"))
        # The per-rule (effective) queue was validated, before submission.
        ex._validate_queue.assert_called_once_with("arn:per-rule-queue")
        assert [c[0] for c in parent.mock_calls] == ["validate", "submit"]

    def test_preexisting_definition_skips_queue_validation(self):
        # Pre-existing job definitions opt out of Describe* permissions, so the
        # queue check must be skipped for them (the job still submits).
        ex = self._executor_for_run_job()
        builder = self._builder(job_queue="arn:q", preexisting=True)
        with patch(
            "snakemake_executor_plugin_aws_batch.BatchJobBuilder",
            return_value=builder,
        ):
            ex.run_job(self._job())
        ex._validate_queue.assert_not_called()
        assert builder.submit.called

    def test_broken_queue_blocks_submission(self):
        ex = self._executor_for_run_job()
        ex._validate_queue.side_effect = WorkflowError("queue is DISABLED")
        builder = self._builder(job_queue="arn:bad")
        with patch(
            "snakemake_executor_plugin_aws_batch.BatchJobBuilder",
            return_value=builder,
        ):
            with pytest.raises(WorkflowError, match="DISABLED"):
                ex.run_job(self._job(batch_queue="arn:bad"))
        # A queue that would never start jobs must abort before submission.
        builder.submit.assert_not_called()
        ex.report_job_submission.assert_not_called()

    def test_validation_error_is_not_rewrapped_as_submit_failure(self):
        # run_job must surface the queue-validation message unwrapped, not
        # re-prefixed with "Failed to submit AWS Batch job" (nothing was
        # submitted). Uses the real _validate_queue against a DISABLED queue.
        ex = self._executor_for_run_job()
        del ex._validate_queue  # use the real method
        ex._queue_validation_cache = {}
        ex.batch_client.describe_job_queues.return_value = {
            "jobQueues": [{"state": "DISABLED", "computeEnvironmentOrder": []}]
        }
        builder = self._builder(job_queue="arn:bad")
        with patch(
            "snakemake_executor_plugin_aws_batch.BatchJobBuilder",
            return_value=builder,
        ):
            with pytest.raises(WorkflowError) as excinfo:
                ex.run_job(self._job(batch_queue="arn:bad"))
        message = str(excinfo.value)
        assert "job queue validation failed" in message
        assert "Failed to submit" not in message
        builder.submit.assert_not_called()


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

    def test_non_client_error_degrades(self):
        # A non-ClientError (e.g. NoCredentialsError — the offline/CI case) must
        # degrade silently rather than raise.
        ex = _executor(job_role="arn:aws:iam::1:role/maybe")
        iam = self._iam(side_effect=RuntimeError("no credentials"))
        with patch("boto3.client", return_value=iam):
            ex._validate_job_role()  # must not raise

    def test_role_without_slash_is_noop(self):
        # A job_role that is not an ARN (no '/') can't be reduced to a role name,
        # so the check is skipped without constructing a client.
        ex = _executor(job_role="bare-role-name")
        with patch("boto3.client") as mocked_client:
            ex._validate_job_role()
            mocked_client.assert_not_called()
