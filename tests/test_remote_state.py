"""Unit tests for the remote-job-state emission helpers.

All tests use synthetic describe_jobs entries — no AWS credentials required.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from snakemake_executor_plugin_aws_batch import remote_state


def _job_info(status, **overrides):
    base = {
        "status": status,
        "jobName": "snakejob-align-7",
        "jobQueue": "arn:aws:batch:us-east-1:1:job-queue/gv-spot",
        "createdAt": 100000,  # ms -> 100.0s
        "container": {},
    }
    base.update(overrides)
    return base


class TestPhaseForStatus:
    def test_queue_states_map_to_queued(self):
        for s in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING"):
            assert remote_state.phase_for_status(s) == "queued"

    def test_running_succeeded_failed(self):
        assert remote_state.phase_for_status("RUNNING") == "running"
        assert remote_state.phase_for_status("SUCCEEDED") == "succeeded"
        assert remote_state.phase_for_status("FAILED") == "failed"

    def test_unknown_status_none(self):
        assert remote_state.phase_for_status("WEIRD") is None
        assert remote_state.phase_for_status(None) is None


class TestBuildPayload:
    def test_running_payload(self):
        info = _job_info(
            "RUNNING",
            startedAt=142000,
            container={"logStreamName": "JobDef/default/abc"},
        )
        payload = remote_state.build_payload(
            snakemake_jobid=7,
            external_jobid="arn:aws:batch:us-east-1:1:job/abc",
            job_info=info,
            region="us-east-1",
        )
        assert payload is not None
        assert payload["schema_version"] == 1
        assert payload["kind"] == "state"
        assert payload["jobid"] == 7
        assert payload["executor"] == "aws-batch"
        assert payload["phase"] == "running"
        assert payload["remote_status"] == "RUNNING"
        assert payload["external_jobid"] == "arn:aws:batch:us-east-1:1:job/abc"
        assert payload["region"] == "us-east-1"
        assert payload["queued_at"] == 100.0  # createdAt 100000 ms
        assert payload["started_at"] == 142.0  # startedAt 142000 ms
        assert payload["queue"] == "arn:aws:batch:us-east-1:1:job-queue/gv-spot"
        assert payload["log_stream"] == "JobDef/default/abc"

    def test_terminal_succeeded_payload(self):
        info = _job_info(
            "SUCCEEDED",
            startedAt=142000,
            stoppedAt=200000,
            container={"exitCode": 0},
            attempts=[{"x": 1}],
        )
        payload = remote_state.build_payload(7, "abc", info, region="us-east-1")
        assert payload is not None
        assert payload["kind"] == "terminal"
        assert payload["phase"] == "succeeded"
        assert payload["stopped_at"] == 200.0
        assert payload["exit_code"] == 0
        # Batch's attempts[] is reported as batch_attempts; "attempt" is
        # Snakemake's and was not supplied here.
        assert payload["batch_attempts"] == 1
        assert "attempt" not in payload

    def test_attempt_is_snakemakes_not_batchs(self):
        # A Snakemake retry is a new Batch job with a single Batch attempt, so
        # the attempt number must come from Snakemake, not from attempts[].
        info = _job_info("RUNNING", attempts=[{"x": 1}])
        payload = remote_state.build_payload(7, "abc", info, attempt=2)
        assert payload is not None
        assert payload["attempt"] == 2
        assert payload["batch_attempts"] == 1

    def test_invalid_attempt_omitted(self):
        for bad in (0, -1, True, "2", 2.0):
            payload = remote_state.build_payload(
                7, "abc", _job_info("RUNNING"), attempt=bad
            )
            assert payload is not None
            assert "attempt" not in payload, bad

    def test_failed_payload_includes_reason(self):
        info = _job_info(
            "FAILED",
            stoppedAt=200000,
            statusReason="Essential container in task exited",
            container={"exitCode": 137},
        )
        payload = remote_state.build_payload(7, "abc", info)
        assert payload is not None
        assert payload["phase"] == "failed"
        assert payload["exit_code"] == 137
        assert payload["status_reason"] == "Essential container in task exited"

    def test_none_when_status_unmappable(self):
        assert remote_state.build_payload(7, "abc", _job_info("WEIRD")) is None

    def test_none_when_no_snakemake_jobid(self):
        assert remote_state.build_payload(None, "abc", _job_info("RUNNING")) is None

    def test_bool_jobid_rejected(self):
        # bool is an int subclass; True/False must not be treated as a job id.
        assert remote_state.build_payload(True, "abc", _job_info("RUNNING")) is None
        assert remote_state.build_payload(False, "abc", _job_info("RUNNING")) is None

    def test_non_numeric_timestamp_omitted_not_raised(self):
        # A malformed AWS timestamp must be dropped, not crash the builder.
        info = _job_info("RUNNING", createdAt="not-a-number")
        payload = remote_state.build_payload(7, "abc", info)
        assert payload is not None
        assert "queued_at" not in payload

    def test_malformed_container_and_attempts_do_not_raise(self):
        # Self-defensive against a non-dict container / non-list attempts.
        info = {"status": "RUNNING", "container": None, "attempts": "weird"}
        payload = remote_state.build_payload(7, "abc", info)
        assert payload is not None
        assert "batch_attempts" not in payload
        assert "log_stream" not in payload

    def test_group_job_uuid_id_emits_string_jobid(self):
        # Group jobs have a UUID string id, not an int. Their transitions must
        # still be emitted, with the string id preserved in the payload.
        uuid_id = "3a7c1e2f-0000-4444-8888-abcdef012345"
        payload = remote_state.build_payload(uuid_id, "abc", _job_info("RUNNING"))
        assert payload is not None
        assert payload["jobid"] == uuid_id
        assert isinstance(payload["jobid"], str)
        assert payload["phase"] == "running"

    def test_group_job_terminal_transition_emitted(self):
        # A group job's terminal transition is emitted too (not dropped).
        uuid_id = "3a7c1e2f-0000-4444-8888-abcdef012345"
        info = _job_info("SUCCEEDED", stoppedAt=200000, container={"exitCode": 0})
        payload = remote_state.build_payload(uuid_id, "abc", info)
        assert payload is not None
        assert payload["jobid"] == uuid_id
        assert payload["kind"] == "terminal"
        assert payload["phase"] == "succeeded"

    def test_empty_string_jobid_rejected(self):
        # An empty/whitespace-only id is not a usable correlation key.
        assert remote_state.build_payload("", "abc", _job_info("RUNNING")) is None
        assert remote_state.build_payload("   ", "abc", _job_info("RUNNING")) is None

    def test_optional_fields_omitted_when_absent(self):
        # A bare queued job with no timestamps/queue still produces a minimal payload.
        info = {"status": "SUBMITTED", "container": {}}
        payload = remote_state.build_payload(7, None, info)
        assert payload is not None
        assert payload["phase"] == "queued"
        assert "external_jobid" not in payload
        assert "started_at" not in payload
        assert "queue" not in payload


class TestEmit:
    def test_emit_attaches_payload_under_wire_key(self):
        import logging

        records = []

        class _Cap(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("test.remote_state.wirekey")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.addHandler(_Cap())
        logger.propagate = False

        payload = {"schema_version": 1, "phase": "running", "jobid": 7}
        remote_state.emit(logger, payload)
        assert len(records) == 1
        assert getattr(records[0], remote_state.WIRE_KEY) is payload

    def test_emit_noop_on_none(self):
        logger = MagicMock()
        remote_state.emit(logger, None)
        logger.info.assert_not_called()

    def test_emit_noop_on_non_stdlib_logger(self):
        # The Snakemake 8 logger is a custom class (info(self, msg, indent=False))
        # that accepts neither *args nor extra=. emit() must skip it cleanly rather
        # than raise TypeError into the caller.
        class _Sm8Logger:  # mimics snakemake.logging.Logger's signature
            def __init__(self):
                self.calls = 0

            def info(self, msg, indent=False):
                self.calls += 1

        logger = _Sm8Logger()
        remote_state.emit(logger, {"phase": "running", "jobid": 7})  # must not raise
        assert logger.calls == 0  # capability gate skipped it, no bad call attempted

    def test_emit_through_real_stdlib_logger_does_not_raise(self):
        # The "doesn't disrupt other users" guarantee: a plain stdlib Logger must
        # accept the message + extra payload without raising, and the payload must
        # land on the record under the wire key.
        import logging

        records = []

        class _Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("test.remote_state.emit")
        logger.setLevel(logging.INFO)
        handler = _Capture()
        logger.addHandler(handler)
        try:
            remote_state.emit(
                logger, {"jobid": 7, "phase": "running", "external_jobid": "abc"}
            )
        finally:
            logger.removeHandler(handler)
        assert len(records) == 1
        assert getattr(records[0], remote_state.WIRE_KEY)["phase"] == "running"


class TestEmitDedup:
    """The executor emits once per phase transition (via job.aux).

    Uses a REAL stdlib logging.Logger with a capturing handler — not a MagicMock —
    so the full emit path (which requires a real logging.Logger for extra=) runs
    and CI would catch a logger-incompatibility regression.
    """

    def _executor_with_logger(self):
        import logging

        from snakemake_executor_plugin_aws_batch import Executor

        ex = Executor.__new__(Executor)
        records = []

        class _Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger(f"test.remote_state.dedup.{id(records)}")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.addHandler(_Capture())
        logger.propagate = False
        ex.logger = logger
        ex.settings = SimpleNamespace(region="us-east-1")
        ex._records = records  # emitted records, for assertions
        return ex

    def _submitted_job(self, jobid=7):
        return SimpleNamespace(
            job=SimpleNamespace(jobid=jobid),
            external_jobid="arn:aws:batch:us-east-1:1:job/abc",
            aux={},
        )

    def test_repeated_same_phase_emits_once(self):
        ex = self._executor_with_logger()
        job = self._submitted_job()
        info = _job_info("RUNNING", startedAt=142000)
        ex._emit_remote_state(job, info)
        ex._emit_remote_state(job, info)  # same phase again -> no second emit
        assert len(ex._records) == 1
        assert job.aux["_remote_state_phase"] == "running"

    def test_emits_snakemakes_attempt_number(self):
        ex = self._executor_with_logger()
        job = SimpleNamespace(
            job=SimpleNamespace(jobid=7, attempt=3),
            external_jobid="arn:aws:batch:us-east-1:1:job/abc",
            aux={},
        )
        ex._emit_remote_state(job, _job_info("RUNNING", startedAt=142000))
        (record,) = ex._records
        assert getattr(record, remote_state.WIRE_KEY)["attempt"] == 3

    def test_phase_change_emits_again(self):
        ex = self._executor_with_logger()
        job = self._submitted_job()
        ex._emit_remote_state(job, _job_info("RUNNABLE"))  # queued
        ex._emit_remote_state(job, _job_info("RUNNING", startedAt=142000))  # running
        ex._emit_remote_state(
            job, _job_info("SUCCEEDED", startedAt=142000, stoppedAt=200000)
        )  # terminal
        assert len(ex._records) == 3

    def test_distinct_queue_substates_dedup_to_one(self):
        # Two DIFFERENT raw Batch statuses that both normalize to "queued" must emit
        # ONCE — proving dedup is on the normalized phase, not the raw status.
        ex = self._executor_with_logger()
        job = self._submitted_job()
        ex._emit_remote_state(job, _job_info("SUBMITTED"))
        ex._emit_remote_state(
            job, _job_info("RUNNABLE")
        )  # different status, same phase
        assert len(ex._records) == 1
        assert job.aux["_remote_state_phase"] == "queued"

    def test_aux_none_is_safe(self):
        ex = self._executor_with_logger()
        job = SimpleNamespace(
            job=SimpleNamespace(jobid=7), external_jobid="abc", aux=None
        )
        ex._emit_remote_state(
            job, _job_info("RUNNING", startedAt=142000)
        )  # must not raise
        assert ex._records == []

    def test_sm8_style_logger_degrades_without_retry_storm(self):
        # A Snakemake-8-style logger (no extra=) must not raise, and the phase must
        # still be recorded so subsequent polls don't re-attempt forever.
        from snakemake_executor_plugin_aws_batch import Executor

        class _Sm8Logger:
            def __init__(self):
                self.info_calls = 0

            def info(self, msg, indent=False):
                self.info_calls += 1

            def debug(self, *a, **k):
                pass

        ex = Executor.__new__(Executor)
        ex.logger = _Sm8Logger()
        ex.settings = SimpleNamespace(region="us-east-1")
        job = self._submitted_job()
        ex._emit_remote_state(job, _job_info("RUNNING", startedAt=142000))  # no raise
        ex._emit_remote_state(job, _job_info("RUNNING", startedAt=142000))
        # Phase recorded (no per-poll retry) and the incompatible logger.info was
        # never called with the unsupported signature.
        assert job.aux["_remote_state_phase"] == "running"
        assert ex.logger.info_calls == 0

    def test_get_job_status_emits_terminal_before_returning(self):
        # Drive the real _get_job_status control flow: a terminal poll must emit a
        # terminal-kind event AND return the exit code. Guards against a refactor
        # that returns before emitting.
        ex = self._executor_with_logger()
        ex.batch_client = MagicMock()
        ex.batch_client.describe_jobs.return_value = {
            "jobs": [
                _job_info(
                    "SUCCEEDED",
                    startedAt=142000,
                    stoppedAt=200000,
                    jobDefinition="arn:def",
                    container={"exitCode": 0},
                )
            ]
        }
        job = self._submitted_job()
        status_code, msg = ex._get_job_status(job)
        assert (status_code, msg) == (0, None)
        assert len(ex._records) == 1
        payload = getattr(ex._records[0], remote_state.WIRE_KEY)
        assert payload["kind"] == "terminal"
        assert payload["phase"] == "succeeded"

    def test_get_job_status_emits_terminal_on_failed_branch(self):
        # The FAILED branch must also emit before returning (the SUCCEEDED test
        # alone left this path uncovered).
        ex = self._executor_with_logger()
        ex.batch_client = MagicMock()
        ex.batch_client.describe_jobs.return_value = {
            "jobs": [
                _job_info(
                    "FAILED",
                    startedAt=142000,
                    stoppedAt=200000,
                    jobDefinition="arn:def",
                    statusReason="Essential container in task exited",
                    container={"exitCode": 137},
                )
            ]
        }
        job = self._submitted_job()
        status_code, msg = ex._get_job_status(job)
        assert status_code == 137
        assert msg == "Essential container in task exited"
        assert len(ex._records) == 1
        payload = getattr(ex._records[0], remote_state.WIRE_KEY)
        assert payload["kind"] == "terminal"
        assert payload["phase"] == "failed"
