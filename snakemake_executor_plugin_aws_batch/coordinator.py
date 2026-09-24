"""Opt-in launcher that runs a workflow's driver ("coordinator") on AWS Batch.

By default you run Snakemake normally — the scheduler runs **locally** and submits
worker jobs to AWS Batch. That ties your terminal (or a login node) to the whole
workflow's lifetime. This launcher is the **opt-in** alternative: it submits a
single Batch job — the *coordinator* — whose command runs Snakemake in the cloud,
then exits, so the local terminal is free to disconnect. The coordinator drives
the workflow with the **full** Snakemake feature set (checkpoints, temp handling,
live scheduling/retries), submitting worker jobs to Batch exactly as a local run
would.

Mechanism — deliberately built on native Snakemake, with no scheduler hijack and
no ``os._exit``:

1. Write a one-rule *wrapper* workflow whose single rule's shell command is your
   real Snakemake invocation.
2. Run that wrapper with ``--executor aws-batch --immediate-submit --notemp``.
   Snakemake natively submits the one job and exits (it does not poll). The
   wrapper rule has **no output**, so Snakemake performs no output verification
   on the fire-and-forget coordinator job.
3. The coordinator job pulls the workflow sources from storage (the plugin sets
   ``job_deploy_sources``) and runs the real workflow.

Before step 1 the launcher dry-runs the real workflow locally and, if Snakemake
reports nothing to be done, exits without submitting — so re-launching a finished
workflow does not pay for a no-op coordinator job. The
``--aws-batch-coordinator-always-submit`` flag skips that check.

Because this is just "submit one Batch job and exit," the local exit code means
*submitted* (or *nothing to do*), not *workflow-succeeded* — monitor the
coordinator via its Batch job status and logs (CloudWatch, and optionally S3 via
the coordinator wrapper script). Run the coordinator on an **on-demand** queue: a
Spot reclaim of the coordinator would take down the whole driver.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

from snakemake.common import NOTHING_TO_BE_DONE_MSG

# Name of the single rule in the generated wrapper workflow. It has no output,
# so Snakemake schedules it whenever targeted by name and never tries to verify
# an output file for the fire-and-forget coordinator job. The name is
# deliberately unlikely so it does not collide with a rule in the included
# real workflow.
COORDINATOR_RULE = "_aws_batch_coordinator_submit"

# A distinctive wrapper-Snakefile name written into the project directory so it
# is deployed to storage alongside the real workflow (job_deploy_sources).
WRAPPER_SNAKEFILE_NAME = ".snakemake_aws_batch_coordinator.smk"

# Interpreter used to invoke the in-job runner INSIDE the coordinator container.
# Must be on PATH there (the published runtime image provides it).
COORDINATOR_INTERPRETER = "python"

# Snakemake's default snakefile search order, used when the workflow args don't
# pass one explicitly (mirrors Snakemake's own resolution, incl. workflow/).
DEFAULT_SNAKEFILE_CANDIDATES = (
    "Snakefile",
    "snakefile",
    "workflow/Snakefile",
    "workflow/snakefile",
)


def _real_snakefile(
    workflow_args: Sequence[str], directory: Optional[Path] = None
) -> str:
    """Extract the real workflow's snakefile path from the passthrough args.

    The wrapper ``include:``s this so Snakemake discovers and deploys the real
    workflow's sources (snakefiles + scripts) with the coordinator job. When no
    ``-s``/``--snakefile`` is given, resolve Snakemake's default search order
    (including the ``workflow/`` layout) against ``directory``.
    """
    args = list(workflow_args)
    for i, arg in enumerate(args):
        if arg in ("-s", "--snakefile") and i + 1 < len(args):
            return args[i + 1]
        if arg.startswith("--snakefile="):
            return arg.split("=", 1)[1]
        if arg.startswith("-s") and len(arg) > 2:  # -sSnakefile
            return arg[2:]
    base = directory or Path.cwd()
    for candidate in DEFAULT_SNAKEFILE_CANDIDATES:
        if (base / candidate).exists():
            return candidate
    return DEFAULT_SNAKEFILE_CANDIDATES[0]


@dataclass(frozen=True)
class CoordinatorConfig:
    """Connection + storage configuration shared by the coordinator and workers."""

    region: str
    job_queue: str
    job_role: str
    default_storage_provider: str
    default_storage_prefix: str
    container_image: Optional[str] = None
    # Queue for the coordinator job itself. Defaults to job_queue, but should be
    # an ON-DEMAND queue: the coordinator is the driver, and a Spot reclaim would
    # tear down the whole run. Workers can still be Spot via job_queue.
    coordinator_queue: Optional[str] = None
    # S3 base for the coordinator's log + metadata + status.json (per run, under
    # AWS_BATCH_JOB_ID). Defaults to "<default_storage_prefix>/.coordinator".
    status_s3_prefix: Optional[str] = None
    # Optional SNS topic ARN for a completion/failure notification.
    notify_sns_topic: Optional[str] = None
    # Optional AWS_BATCH_JOB_ID of a prior coordinator run to resume from: its
    # synced .snakemake/ state is restored before the workflow runs.
    restore_from_job_id: Optional[str] = None
    # Submit the coordinator even when a local dry run reports nothing to do.
    always_submit: bool = False


def strip_remainder_separator(args: Sequence[str]) -> List[str]:
    """Drop the leading ``--`` that ``argparse.REMAINDER`` captures into the list."""
    args = list(args)
    return args[1:] if args and args[0] == "--" else args


def _status_s3_prefix(config: "CoordinatorConfig") -> str:
    if config.status_s3_prefix:
        return config.status_s3_prefix
    return config.default_storage_prefix.rstrip("/") + "/.coordinator"


def _shared_snakemake_args(config: CoordinatorConfig, *, queue: str) -> List[str]:
    """Return the aws-batch + storage flags common to inner and outer commands.

    The caller passes the target ``queue``: the outer (local) submission uses the
    coordinator (on-demand) queue so the coordinator job lands there; the inner
    command (run inside the coordinator) uses the worker job queue.
    """
    args = [
        "--executor",
        "aws-batch",
        "--aws-batch-region",
        config.region,
        "--aws-batch-job-queue",
        queue,
        "--aws-batch-job-role",
        config.job_role,
        "--default-storage-provider",
        config.default_storage_provider,
        "--default-storage-prefix",
        config.default_storage_prefix,
    ]
    if config.container_image:
        # The image is snakemake's global --container-image (there is no
        # container_image executor setting / --aws-batch-container-image flag).
        # On the outer command it sets the coordinator job's image; on the inner
        # command it sets the worker jobs' image.
        args += ["--container-image", config.container_image]
    return args


def _inner_command_parts(
    config: CoordinatorConfig, workflow_args: Sequence[str]
) -> List[str]:
    """The real workflow's Snakemake argv, run inside the coordinator job."""
    return [
        "snakemake",
        *_shared_snakemake_args(config, queue=config.job_queue),
        *workflow_args,
    ]


def build_coordinator_command(
    config: CoordinatorConfig, workflow_args: Sequence[str]
) -> str:
    """Return the wrapper rule's command: the runner wrapping the inner command.

    The coordinator job runs the in-job runner
    (:mod:`snakemake_executor_plugin_aws_batch.coordinator_runner`), which execs
    the real workflow and, on exit, persists the log + ``.snakemake/`` metadata to
    S3 and optionally notifies — surviving an inner-Snakemake crash/OOM.
    """
    runner = [
        COORDINATOR_INTERPRETER,
        "-m",
        "snakemake_executor_plugin_aws_batch.coordinator_runner",
        "--status-s3-prefix",
        _status_s3_prefix(config),
    ]
    if config.notify_sns_topic:
        runner += ["--sns-topic-arn", config.notify_sns_topic]
    if config.restore_from_job_id:
        runner += ["--restore-from-job-id", config.restore_from_job_id]
    runner.append("--")
    return shlex.join(runner + _inner_command_parts(config, workflow_args))


def write_wrapper_snakefile(
    coordinator_command: str, directory: Path, real_snakefile: str
) -> Path:
    """Write the wrapper workflow and return its path.

    The wrapper ``include:``s the real workflow so Snakemake discovers and deploys
    its sources (snakefiles + scripts) with the coordinator job, then defines a
    single **no-output** rule whose command submits the run in the cloud. The
    no-output rule is what lets ``--immediate-submit`` submit-and-exit without
    waiting for an output file. Only the coordinator rule is targeted, so the
    included real rules are parsed but not run locally.
    """
    path = directory / WRAPPER_SNAKEFILE_NAME
    # Refuse to clobber a pre-existing file: launch() deletes the wrapper in its
    # `finally`, so silently truncating a user's own file here would destroy their
    # data on both success and failure.
    if path.exists():
        raise FileExistsError(f"coordinator wrapper path already exists: {path}")
    # Snakemake formats a `shell:` string against the job namespace (wildcards,
    # params, ...), so any literal `{`/`}` in the command — e.g. a brace inside a
    # user `--config` value, storage prefix, or workflow arg — must be doubled or
    # it is parsed as a format field and raises at rule-execution time.
    escaped_command = coordinator_command.replace("{", "{{").replace("}", "}}")
    path.write_text(
        "# Auto-generated by the aws-batch coordinator launcher.\n"
        "# Includes the real workflow (so its sources deploy with the coordinator\n"
        "# job) and submits one no-output rule that runs it in the cloud.\n"
        f"rule {COORDINATOR_RULE}:\n"
        f"    shell:\n"
        f"        {escaped_command!r}\n"
        f"\n"
        f"include: {real_snakefile!r}\n"
    )
    return path


def build_outer_command(
    config: CoordinatorConfig, wrapper_snakefile: Path
) -> List[str]:
    """Build the local Snakemake command that submits the coordinator and exits.

    Uses ``--immediate-submit`` (submit-and-exit, no polling) and ``--notemp``
    (required by immediate-submit), targeting the no-output coordinator rule on
    the coordinator (on-demand) queue.
    """
    return [
        "snakemake",
        *_shared_snakemake_args(
            config, queue=config.coordinator_queue or config.job_queue
        ),
        "--immediate-submit",
        "--notemp",
        "--jobs",
        "1",
        "--snakefile",
        str(wrapper_snakefile),
        COORDINATOR_RULE,
    ]


def build_dry_run_command(
    config: CoordinatorConfig, workflow_args: Sequence[str]
) -> List[str]:
    """Build the local dry run used to decide whether there is anything to do.

    Carries the storage flags (so outputs are checked where the workers write
    them) and the real workflow arguments, but deliberately **not** the executor:
    a dry run only needs the DAG, and initializing the Batch executor locally
    would add AWS calls that decide nothing here.
    """
    return [
        "snakemake",
        "--dry-run",
        "--default-storage-provider",
        config.default_storage_provider,
        "--default-storage-prefix",
        config.default_storage_prefix,
        *workflow_args,
    ]


# Lines of dry-run output echoed when the up-to-date check itself fails.
_DRY_RUN_ERROR_TAIL_LINES = 20


def _workflow_is_up_to_date(
    config: CoordinatorConfig, workflow_args: Sequence[str], runner
) -> bool:
    """Return whether a local dry run reports nothing to be done.

    Snakemake exits 0 whether or not there are jobs, so the decision keys off its
    own "nothing to be done" message (the shared ``NOTHING_TO_BE_DONE_MSG``
    constant, so a wording change upstream is picked up rather than silently
    missed). A failed dry run (e.g. no local storage credentials) returns
    ``False`` with a warning: the check is only an optimization, so it falls back
    to submitting, exactly as without the check.
    """
    completed = runner(
        build_dry_run_command(config, workflow_args),
        capture_output=True,
        text=True,
    )
    output = (completed.stdout or "") + (completed.stderr or "")
    if completed.returncode != 0:
        tail = "\n".join(output.splitlines()[-_DRY_RUN_ERROR_TAIL_LINES:])
        print(
            "Warning: the up-to-date check failed (local `snakemake --dry-run` "
            f"exited {completed.returncode}); submitting the coordinator anyway. "
            "Pass --aws-batch-coordinator-always-submit to skip the check.\n"
            f"{tail}",
            file=sys.stderr,
        )
        return False
    return NOTHING_TO_BE_DONE_MSG in output


def launch(
    config: CoordinatorConfig,
    workflow_args: Sequence[str],
    *,
    project_dir: Optional[Path] = None,
    runner=subprocess.run,
) -> int:
    """Generate the wrapper workflow and submit the coordinator job.

    Unless ``config.always_submit`` is set, first dry-runs the real workflow
    locally and returns 0 without submitting when there is nothing to be done.
    The check is skipped when restoring a prior run, because what a resumed run
    redoes depends on the restored ``.snakemake/`` state that a local dry run
    cannot see.

    Returns the exit code of the local submission run. A zero exit means the
    coordinator was **submitted**, or that there was nothing to submit — not that
    the workflow succeeded. ``runner`` is injectable for testing.
    """
    if not config.always_submit and not config.restore_from_job_id:
        print(
            "Checking whether the workflow has anything to do (local dry run)...",
            file=sys.stderr,
        )
        if _workflow_is_up_to_date(config, workflow_args, runner):
            print(
                "Nothing to be done: all requested outputs are present and up to "
                "date, so not submitting a coordinator job. Pass "
                "--aws-batch-coordinator-always-submit to submit anyway.",
                file=sys.stderr,
            )
            return 0

    directory = project_dir or Path.cwd()
    coordinator_command = build_coordinator_command(config, workflow_args)
    wrapper = write_wrapper_snakefile(
        coordinator_command, directory, _real_snakefile(workflow_args, directory)
    )
    try:
        outer = build_outer_command(config, wrapper)
        print(
            "Submitting the workflow coordinator to AWS Batch "
            "(the terminal can disconnect once it is queued)...",
            file=sys.stderr,
        )
        completed = runner(outer)
        return getattr(completed, "returncode", 0)
    finally:
        wrapper.unlink(missing_ok=True)


def _parse_args(argv: Sequence[str]) -> tuple[CoordinatorConfig, List[str]]:
    parser = argparse.ArgumentParser(
        prog="snakemake-aws-batch-coordinator",
        description=(
            "Submit a Snakemake workflow's driver (coordinator) as an AWS Batch "
            "job and exit. Pass the real workflow arguments after `--`."
        ),
    )
    parser.add_argument(
        "--aws-batch-region", required=True, dest="region", help="AWS region."
    )
    parser.add_argument(
        "--aws-batch-job-queue",
        required=True,
        dest="job_queue",
        help="Batch job queue for the worker jobs the coordinator submits.",
    )
    parser.add_argument(
        "--aws-batch-job-role",
        required=True,
        dest="job_role",
        help="IAM job role ARN passed to the coordinator and worker jobs.",
    )
    parser.add_argument(
        "--aws-batch-coordinator-queue",
        dest="coordinator_queue",
        default=None,
        help="On-demand queue for the coordinator job (defaults to the worker "
        "queue; a Spot reclaim of the coordinator would kill the whole run).",
    )
    parser.add_argument(
        "--aws-batch-container-image",
        dest="container_image",
        default=None,
        help="Container image used to run Snakemake, applied to both the "
        "coordinator job and the worker jobs it submits (maps to snakemake's "
        "--container-image). Defaults to the executor's default image.",
    )
    parser.add_argument(
        "--aws-batch-coordinator-status-prefix",
        dest="status_s3_prefix",
        default=None,
        help="S3 base for the coordinator's log/metadata/status (per run, under "
        "AWS_BATCH_JOB_ID). Defaults to '<default-storage-prefix>/.coordinator'.",
    )
    parser.add_argument(
        "--aws-batch-coordinator-notify-sns-topic",
        dest="notify_sns_topic",
        default=None,
        help="Optional SNS topic ARN for a coordinator completion/failure "
        "notification.",
    )
    parser.add_argument(
        "--aws-batch-coordinator-restore-from-job-id",
        dest="restore_from_job_id",
        default=None,
        help="AWS_BATCH_JOB_ID of a prior coordinator run to resume from: its "
        "synced .snakemake/ state (metadata/ and incomplete/) is restored into "
        "the coordinator workdir before the workflow runs, so it resumes instead "
        "of starting over. The prior run must share this --default-storage-prefix "
        "(and --aws-batch-coordinator-status-prefix, if set).",
    )
    parser.add_argument(
        "--aws-batch-coordinator-always-submit",
        dest="always_submit",
        action="store_true",
        help="Submit the coordinator job even when a local dry run reports that "
        "all requested outputs are up to date. By default the launcher dry-runs "
        "the workflow first (which needs read access to the storage prefix from "
        "the launching machine) and skips submission when there is nothing to do.",
    )
    parser.add_argument(
        "--default-storage-provider",
        required=True,
        help="Snakemake default storage provider (e.g. s3).",
    )
    parser.add_argument(
        "--default-storage-prefix",
        required=True,
        help="Snakemake default storage prefix (e.g. s3://bucket/prefix).",
    )
    parser.add_argument(
        "workflow_args",
        nargs=argparse.REMAINDER,
        help="The real workflow arguments, after `--` (e.g. -s Snakefile targets).",
    )
    ns = parser.parse_args(argv)
    workflow_args = strip_remainder_separator(ns.workflow_args)
    config = CoordinatorConfig(
        region=ns.region,
        job_queue=ns.job_queue,
        job_role=ns.job_role,
        default_storage_provider=ns.default_storage_provider,
        default_storage_prefix=ns.default_storage_prefix,
        container_image=ns.container_image,
        coordinator_queue=ns.coordinator_queue,
        status_s3_prefix=ns.status_s3_prefix,
        notify_sns_topic=ns.notify_sns_topic,
        restore_from_job_id=ns.restore_from_job_id,
        always_submit=ns.always_submit,
    )
    return config, workflow_args


def main(argv: Optional[Sequence[str]] = None) -> int:
    config, workflow_args = _parse_args(sys.argv[1:] if argv is None else argv)
    return launch(config, workflow_args)


if __name__ == "__main__":
    raise SystemExit(main())
