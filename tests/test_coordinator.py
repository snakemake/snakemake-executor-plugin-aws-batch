"""Unit tests for the AWS Batch coordinator launcher.

These cover command construction, wrapper-workflow generation, queue routing,
argument parsing, and that ``launch`` invokes the submission and cleans up the
generated wrapper. They do not touch AWS; the end-to-end "immediate-submit
submits one job and exits" behavior is covered separately by the moto-backed
integration test.
"""

import shlex
from pathlib import Path

import pytest
from snakemake.common import NOTHING_TO_BE_DONE_MSG

from snakemake_executor_plugin_aws_batch.coordinator import (
    COORDINATOR_RULE,
    WRAPPER_SNAKEFILE_NAME,
    CoordinatorConfig,
    _inner_command_parts,
    _parse_args,
    _real_snakefile,
    build_coordinator_command,
    build_dry_run_command,
    build_outer_command,
    launch,
    write_wrapper_snakefile,
)


def _inner_command(config: CoordinatorConfig, workflow_args) -> str:
    """Shell-quoted inner command — the argv the coordinator runs in-cloud."""
    return shlex.join(_inner_command_parts(config, workflow_args))


WORKER_QUEUE = "arn:aws:batch:us-east-1:1:job-queue/workers"
COORD_QUEUE = "arn:aws:batch:us-east-1:1:job-queue/coordinator-ondemand"
ROLE = "arn:aws:iam::1:role/job-role"


def _config(**overrides) -> CoordinatorConfig:
    base = dict(
        region="us-east-1",
        job_queue=WORKER_QUEUE,
        job_role=ROLE,
        default_storage_provider="s3",
        default_storage_prefix="s3://bucket/prefix",
    )
    base.update(overrides)
    return CoordinatorConfig(**base)


class TestInnerCommand:
    def test_inner_uses_worker_queue_and_workflow_args(self):
        cmd = _inner_command(
            _config(coordinator_queue=COORD_QUEUE), ["-s", "Snakefile", "all"]
        )
        assert cmd.startswith("snakemake ")
        assert "--executor aws-batch" in cmd
        # The inner (in-cloud) run submits workers to the WORKER queue.
        assert WORKER_QUEUE in cmd
        assert COORD_QUEUE not in cmd
        assert "--default-storage-provider s3" in cmd
        assert cmd.rstrip().endswith("Snakefile all")

    def test_inner_includes_container_image_when_set(self):
        cmd = _inner_command(_config(container_image="img:1"), ["all"])
        # Image maps to snakemake's global --container-image (there is no
        # --aws-batch-container-image executor flag), so the workers use it.
        assert "--container-image img:1" in cmd
        assert "--aws-batch-container-image" not in cmd


class TestCoordinatorCommand:
    def test_wraps_inner_with_runner_and_default_status_prefix(self):
        cmd = build_coordinator_command(_config(), ["-s", "Snakefile", "all"])
        assert "snakemake_executor_plugin_aws_batch.coordinator_runner" in cmd
        # Default status prefix derives from the storage prefix.
        assert "--status-s3-prefix s3://bucket/prefix/.coordinator" in cmd
        # The real workflow command follows the runner's `--` separator.
        assert " -- snakemake " in cmd
        assert cmd.rstrip().endswith("Snakefile all")

    def test_includes_sns_topic_when_set(self):
        cmd = build_coordinator_command(
            _config(notify_sns_topic="arn:aws:sns:us-east-1:1:topic"), ["all"]
        )
        assert "--sns-topic-arn arn:aws:sns:us-east-1:1:topic" in cmd

    def test_includes_restore_from_job_id_when_set(self):
        cmd = build_coordinator_command(
            _config(restore_from_job_id="job-prior"), ["all"]
        )
        assert "--restore-from-job-id job-prior" in cmd

    def test_omits_restore_from_job_id_by_default(self):
        cmd = build_coordinator_command(_config(), ["all"])
        assert "--restore-from-job-id" not in cmd

    def test_custom_status_prefix_overrides_default(self):
        cmd = build_coordinator_command(
            _config(status_s3_prefix="s3://other/coord"), ["all"]
        )
        assert "--status-s3-prefix s3://other/coord" in cmd
        assert "/.coordinator" not in cmd


class TestOuterCommand:
    def test_outer_is_immediate_submit_notemp_on_coordinator_rule(self):
        outer = build_outer_command(_config(), Path("/tmp/w.smk"))
        assert "--immediate-submit" in outer
        assert "--notemp" in outer
        assert outer[-1] == COORDINATOR_RULE
        assert "--snakefile" in outer and "/tmp/w.smk" in outer

    def test_outer_targets_coordinator_queue_when_set(self):
        outer = build_outer_command(
            _config(coordinator_queue=COORD_QUEUE), Path("/tmp/w.smk")
        )
        # The coordinator JOB itself goes to the on-demand coordinator queue.
        assert COORD_QUEUE in outer
        assert WORKER_QUEUE not in outer

    def test_outer_defaults_coordinator_queue_to_worker_queue(self):
        outer = build_outer_command(_config(), Path("/tmp/w.smk"))
        assert WORKER_QUEUE in outer

    def test_outer_includes_container_image_when_set(self):
        # The image must reach the coordinator job too (docs: applied to both),
        # via snakemake's global --container-image on the outer command.
        outer = build_outer_command(
            _config(container_image="img:1"), Path("/tmp/w.smk")
        )
        assert "--container-image" in outer
        assert "img:1" in outer
        assert "--aws-batch-container-image" not in outer


class TestRealSnakefile:
    def test_extracts_dash_s(self):
        assert _real_snakefile(["-s", "wf/Snakefile", "all"]) == "wf/Snakefile"

    def test_extracts_long_flag_and_equals(self):
        assert _real_snakefile(["--snakefile", "A.smk"]) == "A.smk"
        assert _real_snakefile(["--snakefile=B.smk", "all"]) == "B.smk"
        assert _real_snakefile(["-sC.smk"]) == "C.smk"

    def test_resolves_default_from_directory(self, tmp_path: Path):
        (tmp_path / "workflow").mkdir()
        (tmp_path / "workflow" / "Snakefile").write_text("")
        assert _real_snakefile(["all"], tmp_path) == "workflow/Snakefile"

    def test_defaults_to_snakefile_when_none_exist(self, tmp_path: Path):
        result = _real_snakefile(["all", "--configfile", "c.yaml"], tmp_path)
        assert result == "Snakefile"


class TestWrapperSnakefile:
    def test_includes_real_workflow_and_no_output_rule(self, tmp_path: Path):
        cmd = build_coordinator_command(_config(), ["-s", "Snakefile", "all"])
        path = write_wrapper_snakefile(cmd, tmp_path, "wf/Snakefile")
        assert path.name == WRAPPER_SNAKEFILE_NAME
        text = path.read_text()
        assert f"rule {COORDINATOR_RULE}:" in text
        # The real workflow is included so its sources deploy with the coordinator.
        assert "include: 'wf/Snakefile'" in text or 'include: "wf/Snakefile"' in text
        # No output directive -> nothing for Snakemake to verify on the async job.
        assert "output:" not in text
        assert "shell:" in text

    def test_braces_in_command_are_escaped_for_shell_directive(self, tmp_path: Path):
        # Snakemake brace-formats `shell:` strings, so a literal `{`/`}` from a
        # user --config value must be doubled in the wrapper or it raises at
        # rule-execution time. Route a braced arg through the real builder.
        cmd = build_coordinator_command(_config(), ["--config", "x={a}", "all"])
        assert "{a}" in cmd  # the un-escaped inner command still has a single brace
        path = write_wrapper_snakefile(cmd, tmp_path, "wf/Snakefile")
        text = path.read_text()
        # The shell directive must contain the doubled form, and no lone brace
        # that Snakemake would try to interpolate.
        assert "{{a}}" in text

    def test_refuses_to_overwrite_existing_wrapper(self, tmp_path: Path):
        # If a file with the wrapper name already exists, writing (then deleting
        # it in launch's finally) would destroy user data. Refuse instead.
        cmd = build_coordinator_command(_config(), ["all"])
        existing = tmp_path / WRAPPER_SNAKEFILE_NAME
        existing.write_text("precious user content")
        with pytest.raises(FileExistsError):
            write_wrapper_snakefile(cmd, tmp_path, "wf/Snakefile")
        # The pre-existing file is left untouched.
        assert existing.read_text() == "precious user content"


class _Completed:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class _FakeRunner:
    """Records every command; answers the dry run and the submission separately."""

    def __init__(self, dry_run: _Completed, submit_returncode: int = 0):
        self.dry_run = dry_run
        self.submit_returncode = submit_returncode
        self.calls = []

    def __call__(self, cmd, **kwargs):
        self.calls.append((cmd, kwargs))
        if "--dry-run" in cmd:
            return self.dry_run
        return _Completed(self.submit_returncode)

    @property
    def dry_runs(self):
        return [c for c, _ in self.calls if "--dry-run" in c]

    @property
    def submissions(self):
        return [c for c, _ in self.calls if "--immediate-submit" in c]


# Dry-run output of a workflow that still has jobs to run.
_PENDING_JOBS = "Job stats:\njob      count\n-----  -------\nall          1\n"


class TestDryRunCommand:
    def test_uses_storage_and_workflow_args_without_executor(self):
        cmd = build_dry_run_command(_config(), ["-s", "Snakefile", "all"])
        assert cmd[:2] == ["snakemake", "--dry-run"]
        assert cmd[-3:] == ["-s", "Snakefile", "all"]
        assert cmd[cmd.index("--default-storage-provider") + 1] == "s3"
        assert cmd[cmd.index("--default-storage-prefix") + 1] == "s3://bucket/prefix"
        # A local dry run must not initialize the Batch executor.
        assert "--executor" not in cmd
        assert "--immediate-submit" not in cmd


class TestLaunchUpToDateCheck:
    def test_skips_submission_when_nothing_to_be_done(self, tmp_path: Path, capsys):
        runner = _FakeRunner(_Completed(0, stderr=NOTHING_TO_BE_DONE_MSG + "\n"))
        rc = launch(_config(), ["all"], project_dir=tmp_path, runner=runner)
        assert rc == 0
        assert len(runner.dry_runs) == 1
        assert runner.submissions == []
        # The dry run's output is captured, not streamed, so it can be inspected.
        assert runner.calls[0][1].get("capture_output") is True
        assert not (tmp_path / WRAPPER_SNAKEFILE_NAME).exists()
        assert "not submitting" in capsys.readouterr().err

    def test_submits_when_dry_run_has_pending_jobs(self, tmp_path: Path):
        runner = _FakeRunner(_Completed(0, stdout=_PENDING_JOBS))
        rc = launch(_config(), ["all"], project_dir=tmp_path, runner=runner)
        assert rc == 0
        assert len(runner.dry_runs) == 1
        assert len(runner.submissions) == 1

    def test_submits_with_warning_when_dry_run_fails(self, tmp_path: Path, capsys):
        runner = _FakeRunner(_Completed(1, stderr="MissingCredentials: boom\n"))
        rc = launch(_config(), ["all"], project_dir=tmp_path, runner=runner)
        assert rc == 0
        assert len(runner.submissions) == 1
        err = capsys.readouterr().err
        assert "up-to-date check failed" in err
        assert "MissingCredentials: boom" in err

    def test_always_submit_skips_dry_run(self, tmp_path: Path):
        runner = _FakeRunner(_Completed(0, stderr=NOTHING_TO_BE_DONE_MSG))
        launch(
            _config(always_submit=True), ["all"], project_dir=tmp_path, runner=runner
        )
        assert runner.dry_runs == []
        assert len(runner.submissions) == 1

    def test_restore_skips_dry_run(self, tmp_path: Path):
        # A local dry run cannot see the restored .snakemake/ state (incomplete
        # markers, provenance metadata) that decides what the resumed run redoes.
        runner = _FakeRunner(_Completed(0, stderr=NOTHING_TO_BE_DONE_MSG))
        launch(
            _config(restore_from_job_id="job-prior"),
            ["all"],
            project_dir=tmp_path,
            runner=runner,
        )
        assert runner.dry_runs == []
        assert len(runner.submissions) == 1


class TestLaunch:
    def test_launch_runs_outer_command_and_cleans_up(self, tmp_path: Path):
        calls = {}

        def fake_runner(cmd, **kwargs):
            if "--dry-run" in cmd:
                return _Completed(0, stdout=_PENDING_JOBS)
            calls["cmd"] = cmd
            wrapper = tmp_path / WRAPPER_SNAKEFILE_NAME
            # The wrapper must still exist while the submission runs...
            calls["wrapper_present_during_run"] = wrapper.exists()
            # ...and it must invoke the in-job runner around the real command.
            calls["wrapper_text"] = wrapper.read_text()

            class R:
                returncode = 0

            return R()

        rc = launch(_config(), ["all"], project_dir=tmp_path, runner=fake_runner)
        assert rc == 0
        assert calls["cmd"][0] == "snakemake"
        assert "--immediate-submit" in calls["cmd"]
        assert calls["wrapper_present_during_run"] is True
        assert "coordinator_runner" in calls["wrapper_text"]
        # Wrapper is removed after submission.
        assert not (tmp_path / WRAPPER_SNAKEFILE_NAME).exists()

    def test_launch_propagates_nonzero_exit(self, tmp_path: Path):
        def failing_runner(cmd, **kwargs):
            if "--dry-run" in cmd:
                return _Completed(0, stdout=_PENDING_JOBS)
            return _Completed(2)

        rc = launch(_config(), ["all"], project_dir=tmp_path, runner=failing_runner)
        assert rc == 2
        assert not (tmp_path / WRAPPER_SNAKEFILE_NAME).exists()


class TestParseArgs:
    def test_workflow_args_after_double_dash(self):
        config, workflow_args = _parse_args(
            [
                "--aws-batch-region",
                "us-east-1",
                "--aws-batch-job-queue",
                WORKER_QUEUE,
                "--aws-batch-job-role",
                ROLE,
                "--default-storage-provider",
                "s3",
                "--default-storage-prefix",
                "s3://bucket/prefix",
                "--",
                "-s",
                "Snakefile",
                "all",
            ]
        )
        assert config.region == "us-east-1"
        assert config.job_queue == WORKER_QUEUE
        assert workflow_args == ["-s", "Snakefile", "all"]

    def test_coordinator_queue_optional(self):
        config, _ = _parse_args(
            [
                "--aws-batch-region",
                "us-east-1",
                "--aws-batch-job-queue",
                WORKER_QUEUE,
                "--aws-batch-job-role",
                ROLE,
                "--aws-batch-coordinator-queue",
                COORD_QUEUE,
                "--default-storage-provider",
                "s3",
                "--default-storage-prefix",
                "s3://bucket/prefix",
                "--",
                "all",
            ]
        )
        assert config.coordinator_queue == COORD_QUEUE

    def test_restore_from_job_id_parsed(self):
        config, _ = _parse_args(
            [
                "--aws-batch-region",
                "us-east-1",
                "--aws-batch-job-queue",
                WORKER_QUEUE,
                "--aws-batch-job-role",
                ROLE,
                "--aws-batch-coordinator-restore-from-job-id",
                "job-prior",
                "--default-storage-provider",
                "s3",
                "--default-storage-prefix",
                "s3://bucket/prefix",
                "--",
                "all",
            ]
        )
        assert config.restore_from_job_id == "job-prior"

    def test_always_submit_defaults_off_and_parses(self):
        base = [
            "--aws-batch-region",
            "us-east-1",
            "--aws-batch-job-queue",
            WORKER_QUEUE,
            "--aws-batch-job-role",
            ROLE,
            "--default-storage-provider",
            "s3",
            "--default-storage-prefix",
            "s3://bucket/prefix",
        ]
        config, _ = _parse_args(base + ["--", "all"])
        assert config.always_submit is False
        config, _ = _parse_args(
            base + ["--aws-batch-coordinator-always-submit", "--", "all"]
        )
        assert config.always_submit is True
