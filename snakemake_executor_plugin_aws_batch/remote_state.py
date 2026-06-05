"""Emit a structured remote-job-state event from AWS Batch job status.

A log/monitoring consumer of a Snakemake workflow (a logger plugin, a dashboard,
a TUI) cannot query AWS Batch itself — it is a passive reader of the workflow's
log stream. This module lets the executor *push* the rich state it already learns
from ``describe_jobs`` (queue vs. run phase, the external Batch job id/ARN, the
true execution-window timestamps, exit code, failure reason) to any such consumer,
by attaching a structured payload to an ordinary log record under a well-known
``extra`` key.

A consumer recognizes the key (:data:`WIRE_KEY`, ``"remote_job_state"``) and
translates the payload into an enriched event. Executors/consumers that don't use
it simply never look at it, and every optional field is omitted when absent so a
consumer degrades gracefully. The payload shape is a small versioned contract
(``SCHEMA_VERSION``).
"""

import logging
from typing import Any, Optional, Union

# Log-record attribute / ``extra`` key a consumer looks for.
WIRE_KEY = "remote_job_state"

# Wire-contract version this executor emits.
SCHEMA_VERSION = 1

# AWS Batch status string -> normalized phase.
_STATUS_TO_PHASE = {
    "SUBMITTED": "queued",
    "PENDING": "queued",
    "RUNNABLE": "queued",
    "STARTING": "queued",
    "RUNNING": "running",
    "SUCCEEDED": "succeeded",
    "FAILED": "failed",
}


def phase_for_status(batch_status: Optional[str]) -> Optional[str]:
    """Map an AWS Batch status string to a normalized phase, or None."""
    if batch_status is None:
        return None
    return _STATUS_TO_PHASE.get(batch_status)


def _epoch_seconds(millis: Any) -> Optional[float]:
    """Convert an AWS Batch millisecond timestamp to epoch seconds, or None."""
    if millis is None:
        return None
    try:
        return float(millis) / 1000.0
    except (TypeError, ValueError):
        return None


def _valid_jobid(snakemake_jobid: Any) -> bool:
    """True if ``snakemake_jobid`` is a usable correlation id.

    Single jobs carry an ``int`` id; group jobs carry a UUID/string id. Both are
    accepted. ``bool`` is an ``int`` subclass and is rejected, as are ``None`` and
    empty/whitespace-only strings (no usable id to correlate on).
    """
    if isinstance(snakemake_jobid, bool):
        return False
    if isinstance(snakemake_jobid, int):
        return True
    if isinstance(snakemake_jobid, str):
        return bool(snakemake_jobid.strip())
    return False


def build_payload(
    snakemake_jobid: Optional[Union[int, str]],
    external_jobid: Optional[str],
    job_info: dict,
    region: Optional[str] = None,
    attempt: Optional[int] = None,
) -> Optional[dict]:
    """Build the remote-state payload from a ``describe_jobs`` entry.

    :param snakemake_jobid: Snakemake's job id, used for correlation. An ``int``
        for single jobs, or a UUID/string for group jobs.
    :param external_jobid: The AWS Batch job id/ARN.
    :param job_info: A single entry from ``describe_jobs()["jobs"]``.
    :param region: AWS region, useful for building console deep links.
    :param attempt: Snakemake's 1-based attempt number for this job. Snakemake
        increments it on each ``--retries`` resubmission, and each resubmission
        is a new Batch job, so it is what distinguishes a retry from the job it
        replaces. Omitted from the payload when not a positive ``int``.
    :return: The payload dict, or None if the Batch status can't be mapped to a
        phase, or there is no usable Snakemake job id to correlate on. Group jobs
        (UUID/string ids) are emitted so their transitions are not dropped; the
        ``jobid`` field is the string id in that case.
    """
    phase = phase_for_status(job_info.get("status"))
    # Consumers correlate on Snakemake's job id: an int for single jobs, a
    # UUID/string for group jobs. Reject bool (an int subclass), None, and
    # empty strings, but keep group jobs so their transitions are not dropped.
    if phase is None or not _valid_jobid(snakemake_jobid):
        return None

    # Be self-defensive about the describe_jobs shape: a malformed/partial entry
    # (non-dict container, non-list attempts) must not raise here.
    container = job_info.get("container")
    if not isinstance(container, dict):
        container = {}
    attempts = job_info.get("attempts")
    if not isinstance(attempts, list):
        attempts = []

    # int for single jobs, UUID/string for group jobs.
    correlation_jobid = (
        snakemake_jobid if isinstance(snakemake_jobid, str) else int(snakemake_jobid)
    )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "kind": "terminal" if phase in ("succeeded", "failed") else "state",
        "jobid": correlation_jobid,
        "executor": "aws-batch",
        "phase": phase,
        "remote_status": job_info.get("status"),
    }

    # Optional fields — include only when present so a consumer degrades cleanly.
    if external_jobid is not None:
        payload["external_jobid"] = external_jobid
    if region is not None:
        payload["region"] = region

    queued_at = _epoch_seconds(job_info.get("createdAt"))
    started_at = _epoch_seconds(job_info.get("startedAt"))
    stopped_at = _epoch_seconds(job_info.get("stoppedAt"))
    if queued_at is not None:
        payload["queued_at"] = queued_at
    if started_at is not None:
        payload["started_at"] = started_at
    if stopped_at is not None:
        payload["stopped_at"] = stopped_at

    job_queue = job_info.get("jobQueue")
    if job_queue is not None:
        payload["queue"] = job_queue
    log_stream = container.get("logStreamName")
    if log_stream is not None:
        payload["log_stream"] = log_stream
    if isinstance(attempt, int) and not isinstance(attempt, bool) and attempt >= 1:
        payload["attempt"] = attempt
    if attempts:
        # Batch's own attempts of this one job (its retryStrategy), not Snakemake
        # retries. AWS populates attempts[] while the job is still running, so
        # this is the count *so far*, not necessarily the final one.
        payload["batch_attempts"] = len(attempts)
    exit_code = container.get("exitCode")
    if exit_code is not None:
        payload["exit_code"] = exit_code
    status_reason = job_info.get("statusReason")
    if status_reason is not None:
        payload["status_reason"] = status_reason

    return payload


def emit(logger: Any, payload: Optional[dict]) -> None:
    """Attach a remote-state payload to a log record for a consumer to read.

    No-op when ``payload`` is None. Otherwise emits at INFO level with the payload
    under :data:`WIRE_KEY`; the message text is informational only.

    The structured payload rides on the stdlib logging ``extra=`` mechanism, which
    exists only on Snakemake 9+ (where logger plugins consume it). On Snakemake 8
    the executor's logger is a custom class that accepts neither positional format
    args nor ``extra=`` — and there is no logger-plugin consumer there anyway — so
    this cleanly no-ops rather than raising when handed a non-stdlib logger. That
    keeps emission a best-effort side channel that never disrupts job execution.
    """
    if not payload:
        return
    # Capability gate: only a stdlib logging.Logger accepts (*args, extra=). The
    # Snakemake 8 logger is not one, so skip cleanly instead of raising into the
    # caller (which would otherwise retry on every poll).
    if not isinstance(logger, logging.Logger):
        return
    logger.info(
        "remote job %s -> %s",
        payload.get("external_jobid", payload.get("jobid")),
        payload.get("phase"),
        extra={WIRE_KEY: payload},
    )
