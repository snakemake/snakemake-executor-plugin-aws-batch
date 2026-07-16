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
import threading
from pathlib import Path, PurePosixPath
from typing import List, Optional, Sequence, Tuple

from snakemake_executor_plugin_aws_batch.coordinator import strip_remainder_separator

# .snakemake/ subdirectories worth uploading — the resume-relevant state. An
# allowlist (rather than an exclude list) deliberately skips the heavy dirs that
# can reach gigabytes (conda/, singularity/, shadow/, source_cache/) and the
# process-specific locks/ (restoring which would make a resumed run refuse to
# start).
_METADATA_INCLUDE = {"metadata", "incomplete"}

# Synced subdirs whose remote keys are pruned when absent locally. Only
# incomplete/ markers are harmful when stale (a restore would re-run finished
# work); a stale metadata/ record still describes an output that exists in
# storage, and dropping it would lose provenance an attempt that did not restore
# never rewrote.
_PRUNE_INCLUDE = {"incomplete"}

# Bounded grace period between forwarding SIGTERM to the inner process group and
# escalating to SIGKILL. A child that ignores SIGTERM — or a descendant that keeps
# the stdout pipe open — must not block the tee loop indefinitely, or run() would
# never reach _persist_artifacts() before AWS Batch's own container-stop SIGKILL
# takes down the coordinator mid-persist. That container-stop timeout defaults to
# 30s (the ECS `stopTimeout`), so this grace is kept well below it: the remaining
# budget is what run()'s finally has to upload the log/metadata/status and notify
# before the coordinator itself is killed. Escalating sooner is strictly safer for
# that crash-survival guarantee, since a wedged child only ever delays persistence.
_TERM_GRACE_SECONDS = 15.0


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Split ``s3://bucket/key/prefix`` into ``(bucket, key_prefix)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// URI: {uri}")
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key.rstrip("/")


def _is_within(base: Path, target: Path) -> bool:
    """Return whether ``target`` resolves to a path inside ``base``.

    Resolves both paths (so ``..`` and symlinks are collapsed) and checks
    containment, treating ``base`` itself as inside. Used to keep a restored file
    from escaping ``.snakemake/`` via a crafted S3 key.
    """
    base_resolved = base.resolve()
    target_resolved = target.resolve()
    return target_resolved == base_resolved or base_resolved in target_resolved.parents


def _run_command(inner_command: Sequence[str], log_path: Path) -> int:
    """Run the inner command, teeing combined output to console and ``log_path``.

    Returns the child's exit code. A SIGTERM to this wrapper is forwarded to the
    child's *whole process group* (not just the direct child), then escalated to
    SIGKILL after :data:`_TERM_GRACE_SECONDS`, so a child that ignores SIGTERM or a
    descendant that holds the stdout pipe open cannot block the tee loop and defeat
    the ``finally`` upload in :func:`run`.
    """
    # Decode/encode with utf-8 + errors="replace" so a non-UTF-8 container locale
    # (or genuinely invalid bytes in the driver output) can never raise out of the
    # tee loop and defeat the crash-survival guarantee.
    with open(log_path, "w", encoding="utf-8", errors="replace") as log_file:
        process = subprocess.Popen(
            list(inner_command),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            # Put the child in its own session/process group so we can signal the
            # entire inner process tree, not only the direct child (a no-op on
            # platforms without POSIX sessions).
            start_new_session=True,
        )

        def _signal_tree(sig: int) -> None:
            """Signal the child's whole process group; ignore the exit race."""
            try:
                if hasattr(os, "killpg"):
                    os.killpg(os.getpgid(process.pid), sig)
                else:  # pragma: no cover - non-POSIX has no process groups
                    process.send_signal(sig)
            except (ProcessLookupError, OSError):
                # The process/group already exited between the check and the
                # signal — nothing left to terminate.
                pass

        kill_timer: Optional[threading.Timer] = None

        def _forward_term(signum, frame):
            # Terminate the whole tree, then escalate to SIGKILL after a bounded
            # grace period so an unresponsive child can't wedge the tee loop.
            nonlocal kill_timer
            _signal_tree(signal.SIGTERM)
            sigkill = getattr(signal, "SIGKILL", signal.SIGTERM)
            kill_timer = threading.Timer(
                _TERM_GRACE_SECONDS, _signal_tree, args=(sigkill,)
            )
            kill_timer.daemon = True
            kill_timer.start()

        previous_handler = signal.signal(signal.SIGTERM, _forward_term)
        try:
            assert process.stdout is not None
            # Both sinks are best-effort: a write failure (full disk, broken
            # pipe, closed stream) disables only that sink so output keeps
            # draining and process.wait() can still reap the child. Aborting the
            # loop on a sink error would record a status without the child's real
            # exit code while the child may still be running.
            log_enabled = True
            console_enabled = True
            for line in process.stdout:
                if log_enabled:
                    try:
                        log_file.write(line)
                        log_file.flush()
                    except (OSError, ValueError):
                        log_enabled = False
                if not console_enabled:
                    continue
                # Console echo: under a non-UTF-8 stdout a line with replacement
                # chars raises UnicodeEncodeError, so fall back to the raw byte
                # buffer. Any other I/O failure disables the console sink.
                try:
                    sys.stdout.write(line)
                    sys.stdout.flush()
                except UnicodeEncodeError:
                    buffer = getattr(sys.stdout, "buffer", None)
                    if buffer is None:
                        console_enabled = False
                        continue
                    try:
                        buffer.write(line.encode("utf-8", "replace"))
                        buffer.flush()
                    except (OSError, ValueError):
                        console_enabled = False
                except (OSError, ValueError):
                    console_enabled = False
            return process.wait()
        finally:
            # Cancel the pending SIGKILL if the child already exited cleanly, and
            # restore the previous SIGTERM handler.
            if kill_timer is not None:
                kill_timer.cancel()
            signal.signal(signal.SIGTERM, previous_handler)


def _upload_file(s3, local: Path, bucket: str, key: str) -> None:
    s3.upload_file(str(local), bucket, key)


def _sync_metadata(s3, snakemake_dir: Path, bucket: str, key_prefix: str) -> None:
    """Mirror the resume-relevant ``.snakemake/`` subdirs to ``key_prefix``.

    Walks only the allowlisted subdirs, so the potentially GB-scale
    ``conda/``/``singularity/``/``shadow/`` trees are never traversed.

    AWS Batch retries reuse the job id and therefore ``key_prefix``, so an
    ``incomplete/`` marker left by a failed earlier attempt would otherwise survive
    a later successful attempt and be resurrected by a restore. Once *every* local
    file has uploaded, remote ``incomplete/`` keys absent locally are deleted (see
    :data:`_PRUNE_INCLUDE`). If any upload fails, or there is no local
    ``.snakemake/`` at all, nothing is deleted, so prior state is never dropped
    before a complete replacement exists.
    """
    if not snakemake_dir.is_dir():
        return
    uploaded_keys = set()
    all_uploaded = True
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
                uploaded_keys.add(key)
            except Exception as e:  # one bad file must not abort the sync
                all_uploaded = False
                print(
                    f"coordinator-runner: failed to upload {rel}: {e}", file=sys.stderr
                )
    if not all_uploaded:
        print(
            "coordinator-runner: skipping stale-key cleanup because the snapshot "
            "upload was incomplete",
            file=sys.stderr,
        )
        return
    _delete_stale_metadata(s3, bucket, key_prefix, uploaded_keys)


# S3 DeleteObjects accepts at most this many keys per request.
_DELETE_BATCH_SIZE = 1000


def _delete_stale_metadata(s3, bucket: str, key_prefix: str, keep_keys: set) -> None:
    """Delete prunable keys under ``key_prefix`` that are not in ``keep_keys``.

    Best-effort: a listing or per-key deletion failure is logged, never raised,
    since a leftover stale key is no worse than the pre-sync state.
    """
    prefix = key_prefix.rstrip("/") + "/"
    try:
        stale_keys = [
            obj["Key"]
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix
            )
            for obj in page.get("Contents", [])
            if obj["Key"][len(prefix) :].split("/", 1)[0] in _PRUNE_INCLUDE
            and obj["Key"] not in keep_keys
        ]
    except Exception as e:
        print(
            f"coordinator-runner: failed to list metadata for stale-key cleanup: {e}",
            file=sys.stderr,
        )
        return
    for start in range(0, len(stale_keys), _DELETE_BATCH_SIZE):
        batch = stale_keys[start : start + _DELETE_BATCH_SIZE]
        try:
            response = s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
        except Exception as e:
            print(
                f"coordinator-runner: failed to delete stale metadata: {e}",
                file=sys.stderr,
            )
            continue
        for error in response.get("Errors", []):
            print(
                f"coordinator-runner: failed to delete stale {error.get('Key')}: "
                f"{error.get('Message')}",
                file=sys.stderr,
            )


def _restore_metadata(
    s3, bucket: str, restore_key_prefix: str, snakemake_dir: Path
) -> int:
    """Download a prior run's resume-relevant ``.snakemake/`` state from S3.

    The reverse of :func:`_sync_metadata`: objects previously synced under
    ``restore_key_prefix`` (``<prefix>/<prior job id>/snakemake``) are downloaded
    into ``snakemake_dir`` so Snakemake's ``metadata/`` and ``incomplete/`` are in
    place before the workflow runs, letting the re-run resume instead of redoing
    completed work.

    Restore is best-effort: any object outside the allowlisted subdirs — and any
    key whose relative path would escape ``snakemake_dir`` (absolute or containing
    ``..``) — is skipped, and a per-file failure is logged rather than raised, so a
    partial or failed restore degrades to a from-scratch run instead of aborting.
    Returns the number of files restored.
    """
    prefix = restore_key_prefix.rstrip("/") + "/"
    restored = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(prefix) :]
            # Skip the "directory" placeholder key and anything outside the
            # resume-relevant subdirs (defensive; the sync only writes those).
            if not rel or rel.split("/", 1)[0] not in _METADATA_INCLUDE:
                continue
            # Reject path traversal in the S3 key: a crafted key such as
            # "metadata/../../Snakefile" passes the allowlist above but would let
            # `dest` escape snakemake_dir and overwrite workflow code that then
            # runs with the coordinator's role. Refuse absolute keys and any ".."
            # component, and confirm the resolved destination stays within
            # snakemake_dir before downloading.
            rel_posix = PurePosixPath(rel)
            dest = snakemake_dir / rel
            if (
                rel_posix.is_absolute()
                or ".." in rel_posix.parts
                or not _is_within(snakemake_dir, dest)
            ):
                print(
                    f"coordinator-runner: skipping unsafe restore key {key!r}",
                    file=sys.stderr,
                )
                continue
            try:
                # Directory creation is kept inside the per-object try so a
                # conflict (e.g. FileExistsError when a file sits where a parent
                # dir must go) is logged and skipped like any other per-file
                # failure, rather than aborting the whole restore.
                dest.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, key, str(dest))
                restored += 1
            except Exception as e:  # one bad file must not abort the restore
                print(
                    f"coordinator-runner: failed to restore {rel}: {e}",
                    file=sys.stderr,
                )
    if restored:
        print(
            f"coordinator-runner: restored {restored} file(s) of prior run "
            f"state from s3://{bucket}/{prefix}",
            file=sys.stderr,
        )
    else:
        print(
            f"coordinator-runner: no prior run state found under "
            f"s3://{bucket}/{prefix}; running from scratch",
            file=sys.stderr,
        )
    return restored


def _persist_artifacts(
    boto3_module,
    bucket: str,
    run_key: str,
    job_id: str,
    exit_code: int,
    log_path: Path,
    snakemake_dir: Path,
) -> None:
    """Best-effort upload of the driver log, resume metadata, and a status.json.

    Each artifact is uploaded independently so a failure on one (e.g. the log)
    does not skip the others — in particular ``status.json``, which records the
    outcome and is the most important artifact.
    """
    try:
        s3 = boto3_module.client("s3")
    except Exception as e:  # never mask the workflow's own exit code
        print(f"coordinator-runner: failed to create S3 client: {e}", file=sys.stderr)
        return
    if log_path.exists():
        try:
            _upload_file(s3, log_path, bucket, f"{run_key}/coordinator.log")
        except Exception as e:
            print(f"coordinator-runner: failed to upload log: {e}", file=sys.stderr)
    try:
        _sync_metadata(s3, snakemake_dir, bucket, f"{run_key}/snakemake")
    except Exception as e:
        print(f"coordinator-runner: failed to sync metadata: {e}", file=sys.stderr)
    try:
        status = {"job_id": job_id, "exit_code": exit_code, "succeeded": exit_code == 0}
        s3.put_object(
            Bucket=bucket,
            Key=f"{run_key}/status.json",
            Body=json.dumps(status).encode(),
        )
    except Exception as e:
        print(f"coordinator-runner: failed to write status.json: {e}", file=sys.stderr)


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
    restore_from_job_id: Optional[str] = None,
    workdir: Optional[Path] = None,
    boto3_module=None,
) -> int:
    """Run the coordinator command and persist log + metadata (+ notify) on exit.

    ``status_s3_prefix`` is an ``s3://`` base; artifacts land under
    ``<prefix>/<AWS_BATCH_JOB_ID>/``. When ``restore_from_job_id`` is given, the
    resume-relevant ``.snakemake/`` state persisted by that prior coordinator run
    (``<prefix>/<restore_from_job_id>/snakemake``) is downloaded into ``workdir``
    before the workflow runs, so the re-run resumes rather than starting over.
    Returns the inner command's exit code. ``boto3_module`` is injectable for
    testing.
    """
    if boto3_module is None:
        import boto3 as boto3_module  # local import: only needed at runtime

    directory = workdir or Path.cwd()
    job_id = os.environ.get("AWS_BATCH_JOB_ID", "unknown")
    log_path = directory / "coordinator.log"

    # A malformed status prefix must not abort the run: persistence here is
    # best-effort and the workflow's real outputs go to --default-storage-prefix
    # (separate). Degrade to "no artifact persistence" and still run the workflow.
    bucket: Optional[str]
    restore_key: Optional[str] = None
    try:
        bucket, base_key = _parse_s3_uri(status_s3_prefix)
        run_key = f"{base_key}/{job_id}" if base_key else job_id
        if restore_from_job_id:
            restore_key = (
                f"{base_key}/{restore_from_job_id}" if base_key else restore_from_job_id
            )
    except ValueError as e:
        print(
            f"coordinator-runner: invalid --status-s3-prefix {status_s3_prefix!r}; "
            f"coordinator artifacts will not be persisted: {e}",
            file=sys.stderr,
        )
        bucket, run_key = None, ""

    # Restore a prior run's resume state before running, so the workflow can pick
    # up where it left off. Best-effort: a failure here must not abort the run.
    resuming = restore_key is not None and bucket is not None
    if resuming:
        try:
            _restore_metadata(
                boto3_module.client("s3"),
                bucket,
                f"{restore_key}/snakemake",
                directory / ".snakemake",
            )
        except Exception as e:
            print(
                f"coordinator-runner: failed to restore prior run state "
                f"from job {restore_from_job_id!r}: {e}",
                file=sys.stderr,
            )

    # When resuming, the restored .snakemake/incomplete/ state makes Snakemake
    # refuse to run by default (IncompleteFilesException); --rerun-incomplete lets
    # it re-run those jobs and continue past the incomplete outputs instead.
    command: List[str] = list(inner_command)
    if resuming and "--rerun-incomplete" not in command:
        command.append("--rerun-incomplete")

    exit_code = 1
    try:
        exit_code = _run_command(command, log_path)
        return exit_code
    finally:
        if bucket is not None:
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
        # Invoked as a module inside the coordinator container, not a console
        # script, so name the module form rather than a non-existent entry point.
        prog="python -m snakemake_executor_plugin_aws_batch.coordinator_runner",
        description="Run the coordinator's Snakemake command and persist its log "
        "and metadata to S3 on exit. Pass the command after `--`.",
    )
    parser.add_argument("--status-s3-prefix", required=True)
    parser.add_argument("--sns-topic-arn", default=None)
    parser.add_argument(
        "--restore-from-job-id",
        default=None,
        help="AWS_BATCH_JOB_ID of a prior coordinator run whose resume-relevant "
        ".snakemake/ state (metadata/ and incomplete/) should be downloaded into "
        "the workdir before running, so the workflow resumes instead of starting "
        "over.",
    )
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
        restore_from_job_id=ns.restore_from_job_id,
    )


if __name__ == "__main__":
    raise SystemExit(main())
