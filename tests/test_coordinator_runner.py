"""Unit tests for the in-coordinator runner (log/metadata persistence + notify)."""

import os
import signal
import subprocess
import sys
import time
from unittest.mock import MagicMock

import pytest

import snakemake_executor_plugin_aws_batch.coordinator_runner as cr
from snakemake_executor_plugin_aws_batch.coordinator_runner import (
    _parse_args,
    _parse_s3_uri,
    _restore_metadata,
    _run_command,
    _sync_metadata,
    run,
)


class FakeBoto3:
    """Stand-in boto3 module: `.client("s3"|"sns")` returns per-service mocks."""

    def __init__(self):
        self.s3 = MagicMock(name="s3")
        self.sns = MagicMock(name="sns")

    def client(self, service, **kwargs):
        return {"s3": self.s3, "sns": self.sns}[service]


def _stub_list_objects(s3, keys):
    """Stub s3.get_paginator('list_objects_v2') to yield the given object keys."""
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    s3.get_paginator.return_value = paginator


def test_parse_s3_uri_splits_bucket_and_key():
    assert _parse_s3_uri("s3://bucket/a/b") == ("bucket", "a/b")
    assert _parse_s3_uri("s3://bucket") == ("bucket", "")


def test_run_uploads_log_and_status_and_returns_exit_code(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-42")
    fake = FakeBoto3()
    rc = run(
        [sys.executable, "-c", "print('hello from coordinator')"],
        status_s3_prefix="s3://bucket/base",
        workdir=tmp_path,
        boto3_module=fake,
    )
    assert rc == 0
    # Log file was produced and uploaded under the per-job key.
    assert (tmp_path / "coordinator.log").exists()
    log_keys = [c.args[2] for c in fake.s3.upload_file.call_args_list]
    assert "base/job-42/coordinator.log" in log_keys
    # A status.json recording success was written.
    status_call = fake.s3.put_object.call_args
    assert status_call.kwargs["Key"] == "base/job-42/status.json"
    assert b'"succeeded": true' in status_call.kwargs["Body"]


def test_run_with_bucket_only_prefix_keys_under_job_id(tmp_path, monkeypatch):
    # s3://bucket with no key path -> artifacts land directly under <job_id>/.
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-77")
    fake = FakeBoto3()
    rc = run(
        [sys.executable, "-c", "pass"],
        status_s3_prefix="s3://bucket",
        workdir=tmp_path,
        boto3_module=fake,
    )
    assert rc == 0
    log_keys = [c.args[2] for c in fake.s3.upload_file.call_args_list]
    assert "job-77/coordinator.log" in log_keys
    assert fake.s3.put_object.call_args.kwargs["Key"] == "job-77/status.json"


def test_run_with_invalid_prefix_degrades_but_still_runs(tmp_path, monkeypatch, capsys):
    # A non-s3:// status prefix must NOT abort the workflow: it degrades to no
    # persistence (no S3 calls) but still runs and returns the inner exit code.
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-x")
    fake = FakeBoto3()
    rc = run(
        [sys.executable, "-c", "import sys; sys.exit(5)"],
        status_s3_prefix="/not/an/s3/uri",
        workdir=tmp_path,
        boto3_module=fake,
    )
    assert rc == 5
    fake.s3.upload_file.assert_not_called()
    fake.s3.put_object.assert_not_called()
    assert "will not be persisted" in capsys.readouterr().err


def test_run_reports_failure_exit_code(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-9")
    fake = FakeBoto3()
    rc = run(
        [sys.executable, "-c", "import sys; sys.exit(3)"],
        status_s3_prefix="s3://bucket/base",
        workdir=tmp_path,
        boto3_module=fake,
    )
    assert rc == 3
    assert b'"succeeded": false' in fake.s3.put_object.call_args.kwargs["Body"]


def test_run_publishes_notification_when_topic_set(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-1")
    fake = FakeBoto3()
    run(
        [sys.executable, "-c", "pass"],
        status_s3_prefix="s3://bucket/base",
        sns_topic_arn="arn:aws:sns:us-east-1:1:topic",
        workdir=tmp_path,
        boto3_module=fake,
    )
    assert fake.sns.publish.called
    topic = fake.sns.publish.call_args.kwargs["TopicArn"]
    assert topic == "arn:aws:sns:us-east-1:1:topic"


def test_run_does_not_notify_without_topic(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-1")
    fake = FakeBoto3()
    run(
        [sys.executable, "-c", "pass"],
        status_s3_prefix="s3://bucket/base",
        workdir=tmp_path,
        boto3_module=fake,
    )
    assert not fake.sns.publish.called


def test_sync_metadata_uploads_only_resume_relevant_dirs(tmp_path):
    snakemake_dir = tmp_path / ".snakemake"
    for sub, name in [
        ("metadata", "m1"),
        ("incomplete", "i1"),
        ("locks", "l1"),  # process-specific — must be skipped
        ("conda", "big"),  # heavy cache — must be skipped
        ("shadow", "s1"),  # heavy — must be skipped
    ]:
        (snakemake_dir / sub).mkdir(parents=True)
        (snakemake_dir / sub / name).write_text("x")
    s3 = MagicMock()
    _sync_metadata(s3, snakemake_dir, "bucket", "prefix")
    uploaded_keys = [c.args[2] for c in s3.upload_file.call_args_list]
    assert "prefix/metadata/m1" in uploaded_keys
    assert "prefix/incomplete/i1" in uploaded_keys
    for skipped in ("locks", "conda", "shadow"):
        assert not any(skipped in k for k in uploaded_keys)


def _deleted_keys(s3) -> list:
    """All keys passed to s3.delete_objects across calls."""
    return [
        obj["Key"]
        for c in s3.delete_objects.call_args_list
        for obj in c.kwargs["Delete"]["Objects"]
    ]


def test_sync_metadata_deletes_stale_incomplete_keys_after_uploads(tmp_path):
    # A Batch retry reuses the job id (and so the key prefix): an incomplete/
    # marker left by a failed earlier attempt must not survive a later attempt's
    # sync, or a restore would resurrect it and re-run finished work.
    snakemake_dir = tmp_path / ".snakemake"
    (snakemake_dir / "metadata").mkdir(parents=True)
    (snakemake_dir / "metadata" / "m1").write_text("x")
    (snakemake_dir / "incomplete").mkdir()
    (snakemake_dir / "incomplete" / "i-now").write_text("x")
    s3 = MagicMock()
    order = MagicMock()
    order.attach_mock(s3.upload_file, "upload_file")
    order.attach_mock(s3.delete_objects, "delete_objects")
    _stub_list_objects(
        s3,
        [
            "prefix/incomplete/i-now",  # current — keep
            "prefix/incomplete/i-old",  # stale — delete
            # A stale metadata/ record still describes an output in storage that
            # this attempt may not have rewritten — keep it.
            "prefix/metadata/old",
            "prefix/locks/l1",  # outside the allowlist — never touched
        ],
    )
    _sync_metadata(s3, snakemake_dir, "bucket", "prefix")
    assert _deleted_keys(s3) == ["prefix/incomplete/i-old"]
    s3.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="bucket", Prefix="prefix/"
    )
    # Every upload happens before any delete.
    names = [c[0] for c in order.mock_calls]
    assert names.index("delete_objects") > max(
        i for i, n in enumerate(names) if n == "upload_file"
    )


def test_sync_metadata_deletes_incomplete_keys_when_local_dir_is_gone(tmp_path):
    # Snakemake removes incomplete/ markers as jobs finish, so a clean run can
    # leave no local incomplete/ at all; its stale remote markers must still go.
    snakemake_dir = tmp_path / ".snakemake"
    (snakemake_dir / "metadata").mkdir(parents=True)
    (snakemake_dir / "metadata" / "m1").write_text("x")
    s3 = MagicMock()
    _stub_list_objects(s3, ["prefix/metadata/m1", "prefix/incomplete/i-old"])
    _sync_metadata(s3, snakemake_dir, "bucket", "prefix")
    assert _deleted_keys(s3) == ["prefix/incomplete/i-old"]


def test_sync_metadata_keeps_prior_snapshot_when_an_upload_fails(tmp_path, capsys):
    snakemake_dir = tmp_path / ".snakemake"
    (snakemake_dir / "metadata").mkdir(parents=True)
    (snakemake_dir / "metadata" / "m1").write_text("x")
    (snakemake_dir / "metadata" / "m2").write_text("x")
    s3 = MagicMock()
    s3.upload_file.side_effect = [None, RuntimeError("boom")]
    _stub_list_objects(s3, ["prefix/incomplete/i-old"])
    _sync_metadata(s3, snakemake_dir, "bucket", "prefix")
    s3.delete_objects.assert_not_called()
    s3.get_paginator.assert_not_called()
    assert "skipping stale-key cleanup" in capsys.readouterr().err


def test_sync_metadata_without_snakemake_dir_does_not_delete(tmp_path):
    # Nothing local to mirror (e.g. Snakemake died before creating .snakemake/):
    # keep whatever an earlier attempt persisted rather than wiping it.
    s3 = MagicMock()
    _stub_list_objects(s3, ["prefix/incomplete/i-old"])
    _sync_metadata(s3, tmp_path / ".snakemake", "bucket", "prefix")
    s3.upload_file.assert_not_called()
    s3.delete_objects.assert_not_called()


def test_sync_metadata_batches_deletes_and_logs_errors(tmp_path, capsys):
    snakemake_dir = tmp_path / ".snakemake"
    snakemake_dir.mkdir()
    s3 = MagicMock()
    stale = [f"prefix/incomplete/i{n}" for n in range(1001)]
    _stub_list_objects(s3, stale)
    s3.delete_objects.return_value = {
        "Errors": [{"Key": stale[0], "Message": "denied"}]
    }
    _sync_metadata(s3, snakemake_dir, "bucket", "prefix")
    # S3 caps DeleteObjects at 1000 keys per request.
    assert [
        len(c.kwargs["Delete"]["Objects"]) for c in s3.delete_objects.call_args_list
    ] == [1000, 1]
    assert sorted(_deleted_keys(s3)) == sorted(stale)
    assert f"failed to delete stale {stale[0]}" in capsys.readouterr().err


def test_parse_args_inner_command_after_double_dash():
    ns, inner = _parse_args(
        ["--status-s3-prefix", "s3://b/p", "--", "snakemake", "-s", "Snakefile"]
    )
    assert ns.status_s3_prefix == "s3://b/p"
    assert ns.restore_from_job_id is None
    assert inner == ["snakemake", "-s", "Snakefile"]


def test_parse_args_restore_from_job_id():
    ns, inner = _parse_args(
        [
            "--status-s3-prefix",
            "s3://b/p",
            "--restore-from-job-id",
            "job-prior",
            "--",
            "snakemake",
        ]
    )
    assert ns.restore_from_job_id == "job-prior"
    assert inner == ["snakemake"]


def test_restore_metadata_downloads_only_resume_relevant_dirs(tmp_path):
    snakemake_dir = tmp_path / ".snakemake"
    s3 = MagicMock()
    _stub_list_objects(
        s3,
        [
            "base/job-prior/snakemake/",  # placeholder key -> skipped
            "base/job-prior/snakemake/metadata/m1",
            "base/job-prior/snakemake/incomplete/i1",
            "base/job-prior/snakemake/locks/l1",  # not allowlisted -> skipped
        ],
    )
    restored = _restore_metadata(
        s3, "bucket", "base/job-prior/snakemake", snakemake_dir
    )
    assert restored == 2
    downloaded_keys = [c.args[1] for c in s3.download_file.call_args_list]
    assert "base/job-prior/snakemake/metadata/m1" in downloaded_keys
    assert "base/job-prior/snakemake/incomplete/i1" in downloaded_keys
    assert not any("locks" in k for k in downloaded_keys)
    # Destinations are under the local .snakemake/ dir, mirroring the S3 layout.
    dests = [c.args[2] for c in s3.download_file.call_args_list]
    assert str(snakemake_dir / "metadata" / "m1") in dests


def test_restore_metadata_rejects_path_traversal_keys(tmp_path, capsys):
    # A crafted key whose first component is allowlisted but which contains ".."
    # would escape .snakemake/ and could overwrite workflow code that then runs
    # with the coordinator's role. Such keys must be skipped, not downloaded.
    snakemake_dir = tmp_path / ".snakemake"
    s3 = MagicMock()
    _stub_list_objects(
        s3,
        [
            "base/job-prior/snakemake/metadata/../../Snakefile",  # traversal
            "base/job-prior/snakemake/metadata/m1",  # safe -> downloaded
        ],
    )
    restored = _restore_metadata(
        s3, "bucket", "base/job-prior/snakemake", snakemake_dir
    )
    assert restored == 1
    downloaded_keys = [c.args[1] for c in s3.download_file.call_args_list]
    assert "base/job-prior/snakemake/metadata/m1" in downloaded_keys
    assert not any(".." in k for k in downloaded_keys)
    # The unsafe key is reported so an operator can see it was skipped.
    assert "unsafe" in capsys.readouterr().err
    # No file escaped the .snakemake/ dir into the project root.
    assert not (tmp_path / "Snakefile").exists()


def test_restore_metadata_directory_conflict_does_not_abort_restore(tmp_path, capsys):
    # A directory-creation failure on one object (here a file sitting where a
    # parent dir must go) must be logged and skipped like any other per-file
    # failure, letting the remaining objects restore — not abort the whole run.
    snakemake_dir = tmp_path / ".snakemake"
    # Pre-create metadata/ as a *file* so dest.parent.mkdir() raises FileExistsError
    # for keys under metadata/, while incomplete/ can still be created normally.
    snakemake_dir.mkdir(parents=True)
    (snakemake_dir / "metadata").write_text("not a directory")
    s3 = MagicMock()
    _stub_list_objects(
        s3,
        [
            "base/job-prior/snakemake/metadata/m1",  # mkdir conflict -> skipped
            "base/job-prior/snakemake/incomplete/i1",  # restores fine
        ],
    )
    restored = _restore_metadata(
        s3, "bucket", "base/job-prior/snakemake", snakemake_dir
    )
    assert restored == 1
    downloaded_keys = [c.args[1] for c in s3.download_file.call_args_list]
    assert downloaded_keys == ["base/job-prior/snakemake/incomplete/i1"]
    # The conflicting object is reported as a per-file failure, not raised.
    assert "failed to restore metadata/m1" in capsys.readouterr().err


def test_run_restores_prior_state_before_running(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-new")
    fake = FakeBoto3()
    _stub_list_objects(fake.s3, ["base/job-old/snakemake/metadata/m1"])
    rc = run(
        [sys.executable, "-c", "pass"],
        status_s3_prefix="s3://bucket/base",
        restore_from_job_id="job-old",
        workdir=tmp_path,
        boto3_module=fake,
    )
    assert rc == 0
    # The prior run's metadata was downloaded before the workflow ran.
    downloaded_keys = [c.args[1] for c in fake.s3.download_file.call_args_list]
    assert "base/job-old/snakemake/metadata/m1" in downloaded_keys


def test_run_without_restore_does_not_list_or_download(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-new")
    fake = FakeBoto3()
    run(
        [sys.executable, "-c", "pass"],
        status_s3_prefix="s3://bucket/base",
        workdir=tmp_path,
        boto3_module=fake,
    )
    fake.s3.get_paginator.assert_not_called()
    fake.s3.download_file.assert_not_called()


# Inner command that records its own argv (beyond the dump path) to a file, so a
# test can assert which flags run() forwarded to the workflow invocation.
_ARGV_DUMP = (
    "import sys,pathlib;"
    "pathlib.Path(sys.argv[1]).write_text(chr(10).join(sys.argv[2:]))"
)


def test_run_appends_rerun_incomplete_when_resuming(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-new")
    fake = FakeBoto3()
    _stub_list_objects(fake.s3, ["base/job-old/snakemake/incomplete/i1"])
    argv_file = tmp_path / "argv.txt"
    rc = run(
        [sys.executable, "-c", _ARGV_DUMP, str(argv_file)],
        status_s3_prefix="s3://bucket/base",
        restore_from_job_id="job-old",
        workdir=tmp_path,
        boto3_module=fake,
    )
    assert rc == 0
    # Resuming replays the restored incomplete/ state, which Snakemake refuses to
    # run without --rerun-incomplete, so run() must have appended it.
    assert "--rerun-incomplete" in argv_file.read_text().splitlines()


def test_run_does_not_append_rerun_incomplete_without_restore(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-new")
    fake = FakeBoto3()
    argv_file = tmp_path / "argv.txt"
    rc = run(
        [sys.executable, "-c", _ARGV_DUMP, str(argv_file)],
        status_s3_prefix="s3://bucket/base",
        workdir=tmp_path,
        boto3_module=fake,
    )
    assert rc == 0
    # A fresh (non-resume) run has no incomplete state to rerun, so the flag must
    # not be injected.
    assert "--rerun-incomplete" not in argv_file.read_text().splitlines()


# Helper run in a child process: it invokes run() with a long-sleeping inner
# command that first records its own PID, so the parent test can SIGTERM the
# wrapper and confirm the inner child was terminated rather than orphaned.
_SIGTERM_HELPER = """
import os, sys, time
from pathlib import Path
from unittest.mock import MagicMock
from snakemake_executor_plugin_aws_batch.coordinator_runner import run

pidfile, workdir, rcfile = sys.argv[1], sys.argv[2], sys.argv[3]
os.environ["AWS_BATCH_JOB_ID"] = "sigterm-test"
inner = [
    sys.executable, "-c",
    "import os,sys,time;"
    "open(sys.argv[1],'w').write(str(os.getpid()));"
    "time.sleep(120)",
    pidfile,
]
print("READY", flush=True)
rc = run(inner, status_s3_prefix="s3://b/p", workdir=Path(workdir),
         boto3_module=MagicMock())
Path(rcfile).write_text(str(rc))
"""


def _wait_until(predicate, timeout, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@pytest.mark.skipif(
    os.name != "posix", reason="SIGTERM forwarding is a POSIX-signal behavior"
)
def test_run_command_forwards_sigterm_to_child(tmp_path):
    pidfile = tmp_path / "child.pid"
    rcfile = tmp_path / "rc.txt"
    helper = tmp_path / "sigterm_helper.py"
    helper.write_text(_SIGTERM_HELPER)

    proc = subprocess.Popen(
        [sys.executable, str(helper), str(pidfile), str(tmp_path), str(rcfile)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "READY"
        # Wait for the inner child to record its PID. The helper creates the file
        # and writes the PID in two steps, so require numeric content — not mere
        # existence — before parsing, or an empty read races to a ValueError.
        assert _wait_until(
            lambda: pidfile.exists() and pidfile.read_text().strip().isdigit(),
            timeout=15,
        ), "child never started"
        child_pid = int(pidfile.read_text())

        # SIGTERM the wrapper; _forward_term must terminate the inner child.
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=15)

        # A broken forward would orphan the inner child (still sleeping 120s);
        # a working forward terminates and reaps it.
        assert _wait_until(
            lambda: not _process_alive(child_pid), timeout=10
        ), "inner child was orphaned — SIGTERM was not forwarded"
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.stdout.close()


# Helper run in a child process: the inner command *ignores* SIGTERM (installs
# SIG_IGN) and sleeps, so a plain terminate() would leave it — and the wrapper's
# read loop — blocked. The wrapper must escalate to SIGKILL after a short grace
# period so run() still finishes and persists artifacts.
_SIGTERM_IGNORING_HELPER = """
import os, sys, signal, time
from pathlib import Path
from unittest.mock import MagicMock
import snakemake_executor_plugin_aws_batch.coordinator_runner as cr

# Escalate quickly so the test does not wait the production grace period.
cr._TERM_GRACE_SECONDS = 2.0

pidfile, workdir, rcfile = sys.argv[1], sys.argv[2], sys.argv[3]
os.environ["AWS_BATCH_JOB_ID"] = "sigkill-test"
inner = [
    sys.executable, "-c",
    "import os,sys,signal,time;"
    "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
    "open(sys.argv[1],'w').write(str(os.getpid()));"
    "time.sleep(120)",
    pidfile,
]
print("READY", flush=True)
rc = cr.run(inner, status_s3_prefix="s3://b/p", workdir=Path(workdir),
            boto3_module=MagicMock())
Path(rcfile).write_text(str(rc))
"""


class _FailingLog:
    """Log sink stand-in whose writes always raise, as a full disk would."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def write(self, _line):
        raise OSError("No space left on device")

    def flush(self):
        raise OSError("No space left on device")


def test_run_command_survives_log_write_failure(tmp_path, monkeypatch, capsys):
    """A failing log sink must not abort child supervision.

    If ``log_file.write()`` raises (e.g. ENOSPC), the tee loop must disable only
    that sink, keep draining the child's output, and still reap the child —
    returning the child's exit code rather than propagating the I/O error and
    recording a status without the child's real exit status.
    """
    monkeypatch.setattr(cr, "open", lambda *a, **k: _FailingLog(), raising=False)
    inner = [
        sys.executable,
        "-c",
        "import sys; sys.stdout.write('hello\\n'); sys.stdout.flush(); sys.exit(7)",
    ]

    rc = _run_command(inner, tmp_path / "log.txt")

    assert rc == 7
    # The console sink stays live even though the log sink died.
    assert "hello" in capsys.readouterr().out


@pytest.mark.skipif(
    os.name != "posix", reason="process-group SIGKILL escalation is POSIX-only"
)
def test_run_command_escalates_to_sigkill_when_sigterm_ignored(tmp_path):
    pidfile = tmp_path / "child.pid"
    rcfile = tmp_path / "rc.txt"
    helper = tmp_path / "sigkill_helper.py"
    helper.write_text(_SIGTERM_IGNORING_HELPER)

    proc = subprocess.Popen(
        [sys.executable, str(helper), str(pidfile), str(tmp_path), str(rcfile)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "READY"
        # Require numeric content, not mere existence: the file is created before
        # the PID is written, so an existence-only wait can read it empty.
        assert _wait_until(
            lambda: pidfile.exists() and pidfile.read_text().strip().isdigit(),
            timeout=15,
        ), "child never started"
        child_pid = int(pidfile.read_text())

        # SIGTERM the wrapper; the inner child ignores SIGTERM, so only the
        # bounded SIGKILL escalation can free the read loop.
        proc.send_signal(signal.SIGTERM)
        # The wrapper must exit on its own (grace ~2s + reap), not hang for 120s.
        proc.wait(timeout=30)

        assert _wait_until(
            lambda: not _process_alive(child_pid), timeout=10
        ), "inner child survived — SIGKILL escalation did not fire"
        # run() completed its finally block and recorded an exit code.
        assert _wait_until(lambda: rcfile.exists(), timeout=10), "run() never returned"
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.stdout.close()
