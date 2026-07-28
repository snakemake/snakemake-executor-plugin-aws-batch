"""In-coordinator wrapper: run Snakemake, then persist its log + metadata + notify.

This runs *inside* the coordinator Batch job, wrapping the real Snakemake
invocation. Because it is the **parent** process, it survives an inner-Snakemake
crash or OOM and still, on exit:

1. uploads the driver's combined stdout/stderr log to S3,
2. syncs the resume-relevant ``.snakemake/`` state to S3 (``metadata/`` and
   ``incomplete/`` — deliberately skipping ``locks/`` and heavy caches like
   ``conda/``/``singularity/``/``shadow/``), and
3. optionally publishes an SNS notification carrying the outcome.

Everything is written under ``<status prefix>/<AWS_BATCH_JOB_ID>/`` so each
coordinator run is isolated. Only an instance-level failure (e.g. a Spot reclaim
of the coordinator host, which sends SIGKILL with no grace) escapes this — a
reason to run the coordinator on on-demand capacity.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from snakemake_executor_plugin_aws_batch.coordinator import strip_remainder_separator

# .snakemake/ subdirectories worth uploading — the resume-relevant state. An
# allowlist (rather than an exclude list) deliberately skips the heavy dirs that
# can reach gigabytes (conda/, singularity/, shadow/, source_cache/) and the
# process-specific locks/ (restoring which would make a resumed run refuse to
# start).
_METADATA_INCLUDE = {"metadata", "incomplete"}


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Split ``s3://bucket/key/prefix`` into ``(bucket, key_prefix)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// URI: {uri}")
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key.rstrip("/")


def _run_command(inner_command: Sequence[str], log_path: Path) -> int:
    """Run the inner command, teeing combined output to console and ``log_path``.

    Returns the child's exit code. A SIGTERM to this wrapper is forwarded to the
    child so the ``finally`` upload in :func:`run` still executes.
    """
    with open(log_path, "w") as log_file:
        process = subprocess.Popen(
            list(inner_command),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        def _forward_term(signum, frame):
            process.terminate()

        previous_handler = signal.signal(signal.SIGTERM, _forward_term)
        try:
            assert process.stdout is not None
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log_file.write(line)
                log_file.flush()
            return process.wait()
        finally:
            signal.signal(signal.SIGTERM, previous_handler)


def _upload_file(s3, local: Path, bucket: str, key: str) -> None:
    s3.upload_file(str(local), bucket, key)


def _sync_metadata(s3, snakemake_dir: Path, bucket: str, key_prefix: str) -> None:
    """Upload the resume-relevant ``.snakemake/`` subdirs under ``key_prefix``.

    Walks only the allowlisted subdirs, so the potentially GB-scale
    ``conda/``/``singularity/``/``shadow/`` trees are never traversed.
    """
    for subdir in _METADATA_INCLUDE:
        root = snakemake_dir / subdir
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(snakemake_dir)
            key = f"{key_prefix}/{rel.as_posix()}"
            try:
                _upload_file(s3, path, bucket, key)
            except Exception as e:  # one bad file must not abort the sync
                print(
                    f"coordinator-runner: failed to upload {rel}: {e}", file=sys.stderr
                )


def _persist_artifacts(
    boto3_module,
    bucket: str,
    run_key: str,
    job_id: str,
    exit_code: int,
    log_path: Path,
    snakemake_dir: Path,
) -> None:
    """Best-effort upload of the driver log, resume metadata, and a status.json."""
    try:
        s3 = boto3_module.client("s3")
        if log_path.exists():
            _upload_file(s3, log_path, bucket, f"{run_key}/coordinator.log")
        _sync_metadata(s3, snakemake_dir, bucket, f"{run_key}/snakemake")
        status = {"job_id": job_id, "exit_code": exit_code, "succeeded": exit_code == 0}
        s3.put_object(
            Bucket=bucket,
            Key=f"{run_key}/status.json",
            Body=json.dumps(status).encode(),
        )
    except Exception as e:  # never mask the workflow's own exit code
        print(f"coordinator-runner: failed to persist artifacts: {e}", file=sys.stderr)


def _notify(
    boto3_module, sns_topic_arn: str, job_id: str, exit_code: int, status_s3_prefix: str
) -> None:
    """Best-effort SNS notification of the coordinator's outcome."""
    try:
        outcome = "succeeded" if exit_code == 0 else "FAILED"
        boto3_module.client("sns").publish(
            TopicArn=sns_topic_arn,
            Subject=f"Snakemake coordinator {job_id} {outcome}",
            Message=(
                f"Coordinator job {job_id} {outcome} (exit code {exit_code}).\n"
                f"Artifacts: {status_s3_prefix.rstrip('/')}/{job_id}/"
            ),
        )
    except Exception as e:
        print(
            f"coordinator-runner: failed to publish notification: {e}", file=sys.stderr
        )


def run(
    inner_command: Sequence[str],
    *,
    status_s3_prefix: str,
    sns_topic_arn: Optional[str] = None,
    workdir: Optional[Path] = None,
    boto3_module=None,
) -> int:
    """Run the coordinator command and persist log + metadata (+ notify) on exit.

    ``status_s3_prefix`` is an ``s3://`` base; artifacts land under
    ``<prefix>/<AWS_BATCH_JOB_ID>/``. Returns the inner command's exit code.
    ``boto3_module`` is injectable for testing.
    """
    if boto3_module is None:
        import boto3 as boto3_module  # local import: only needed at runtime

    directory = workdir or Path.cwd()
    job_id = os.environ.get("AWS_BATCH_JOB_ID", "unknown")
    bucket, base_key = _parse_s3_uri(status_s3_prefix)
    run_key = f"{base_key}/{job_id}" if base_key else job_id
    log_path = directory / "coordinator.log"

    exit_code = 1
    try:
        exit_code = _run_command(inner_command, log_path)
        return exit_code
    finally:
        _persist_artifacts(
            boto3_module,
            bucket,
            run_key,
            job_id,
            exit_code,
            log_path,
            directory / ".snakemake",
        )
        if sns_topic_arn:
            _notify(boto3_module, sns_topic_arn, job_id, exit_code, status_s3_prefix)


def _parse_args(argv: Sequence[str]) -> Tuple[argparse.Namespace, List[str]]:
    parser = argparse.ArgumentParser(
        prog="snakemake-aws-batch-coordinator-runner",
        description="Run the coordinator's Snakemake command and persist its log "
        "and metadata to S3 on exit. Pass the command after `--`.",
    )
    parser.add_argument("--status-s3-prefix", required=True)
    parser.add_argument("--sns-topic-arn", default=None)
    parser.add_argument("inner_command", nargs=argparse.REMAINDER)
    ns = parser.parse_args(argv)
    return ns, strip_remainder_separator(ns.inner_command)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ns, inner = _parse_args(sys.argv[1:] if argv is None else argv)
    if not inner:
        print("coordinator-runner: no command given after `--`", file=sys.stderr)
        return 2
    return run(
        inner,
        status_s3_prefix=ns.status_s3_prefix,
        sns_topic_arn=ns.sns_topic_arn,
    )


if __name__ == "__main__":
    raise SystemExit(main())
