"""Unit tests for the AWS Batch coordinator launcher.

These cover command construction, wrapper-workflow generation, queue routing,
argument parsing, and that ``launch`` invokes the submission and cleans up the
generated wrapper. They do not touch AWS; the end-to-end "immediate-submit
submits one job and exits" behavior is covered separately by the moto-backed
integration test.
"""

import shlex
from pathlib import Path

from snakemake_executor_plugin_aws_batch.coordinator import (
    COORDINATOR_RULE,
    WRAPPER_SNAKEFILE_NAME,
    CoordinatorConfig,
    _inner_command_parts,
    _parse_args,
    _real_snakefile,
    build_coordinator_command,
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
        assert "--aws-batch-container-image img:1" in cmd


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


class TestLaunch:
    def test_launch_runs_outer_command_and_cleans_up(self, tmp_path: Path):
        calls = {}

        def fake_runner(cmd, **kwargs):
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
            class R:
                returncode = 2

            return R()

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
