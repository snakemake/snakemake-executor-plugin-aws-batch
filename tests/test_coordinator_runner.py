"""Unit tests for the in-coordinator runner (log/metadata persistence + notify)."""

import os
import signal
import subprocess
import sys
import time
from unittest.mock import MagicMock

import pytest

from snakemake_executor_plugin_aws_batch.coordinator_runner import (
    _parse_args,
    _parse_s3_uri,
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


def test_parse_args_inner_command_after_double_dash():
    ns, inner = _parse_args(
        ["--status-s3-prefix", "s3://b/p", "--", "snakemake", "-s", "Snakefile"]
    )
    assert ns.status_s3_prefix == "s3://b/p"
    assert inner == ["snakemake", "-s", "Snakefile"]


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
        # Wait for the inner child to record its PID.
        assert _wait_until(lambda: pidfile.exists(), timeout=15), "child never started"
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
