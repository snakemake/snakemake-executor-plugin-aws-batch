"""Unit tests for the rule queue map (--aws-batch-rule-queues)."""

import dataclasses
import json
import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.jobs import GroupJobExecutorInterface

from snakemake_executor_plugin_aws_batch import Executor, ExecutorSettings
from snakemake_executor_plugin_aws_batch.batch_job_builder import BatchJobBuilder
from snakemake_executor_plugin_aws_batch.rule_queues import RuleQueues

GPU = "arn:aws:batch:us-east-1:123456789012:job-queue/gpu"
CPU = "cpu-spot"


def _job(name: str, **resources) -> SimpleNamespace:
    return SimpleNamespace(name=name, resources=resources)


def _group(*rules: str) -> MagicMock:
    group = MagicMock(spec=GroupJobExecutorInterface)
    group.name = "g1_" + "_".join(sorted(set(rules)))
    group.jobs = [_job(rule) for rule in rules]
    group.resources = {}
    return group


# ---------------------------------------------------------------------------
# Parsing and validating the map
# ---------------------------------------------------------------------------


def test_a_rule_in_the_map_gets_its_queue():
    queues = RuleQueues({"align": GPU, "call": CPU})
    assert queues.queue_of(_job("align")) == GPU
    assert queues.queue_of(_job("call")) == CPU
    assert len(queues) == 2


def test_a_rule_not_in_the_map_gets_none():
    assert RuleQueues({"align": GPU}).queue_of(_job("other")) is None


def test_an_empty_map_gives_no_queues():
    queues = RuleQueues.from_json("{}")
    assert len(queues) == 0
    assert queues.queue_of(_job("align")) is None


@pytest.mark.parametrize(
    "queue",
    [
        "q",
        "a" * 128,
        "cpu_spot-2",
        GPU,
        "arn:aws-us-gov:batch:us-gov-west-1:123456789012:job-queue/q",
        "arn:aws-cn:batch:cn-north-1:123456789012:job-queue/q",
    ],
)
def test_job_queue_names_and_arns_are_accepted(queue):
    assert RuleQueues({"r": queue}).queue_of(_job("r")) == queue


@pytest.mark.parametrize(
    "mapping, want",
    [
        ({"": CPU}, "rule queue map key '' is not a rule name"),
        ({"r": ""}, "rule queue map entry 'r': '' is not a job queue name or job"),
        ({"r": " q"}, "rule queue map entry 'r': ' q' is not a job queue name"),
        ({"r": "a" * 129}, "is not a job queue name or job queue ARN"),
        ({"r": 3}, "rule queue map entry 'r': 3 is not a job queue name"),
        ({"r": None}, "rule queue map entry 'r': None is not a job queue name"),
        (
            {"r": "arn:aws:batch:us-east-1:123:job-queue/q"},
            "is not a job queue name or job queue ARN",
        ),
        (
            {"r": "arn:aws:ecs:us-east-1:123456789012:job-queue/q"},
            "is not a job queue name or job queue ARN",
        ),
        (
            {"r": "arn:aws:batch:us-east-1:123456789012:job-definition/q:1"},
            "is not a job queue name or job queue ARN",
        ),
    ],
)
def test_invalid_entries_are_refused(mapping, want):
    with pytest.raises(ValueError, match=re.escape(want)):
        RuleQueues(mapping)


@pytest.mark.parametrize("rule", ["1", "3", "align-bwa-mem", "align.bwa", "class"])
def test_any_snakemake_rule_name_is_a_key(rule):
    # Snakemake names an unnamed rule by its number and takes any string from a
    # name: directive, so the map cannot require a Python identifier.
    assert RuleQueues({rule: CPU}).queue_of(_job(rule)) == CPU


def test_a_refused_value_names_the_accepted_forms():
    want = (
        "rule queue map entry 'r': 'a b' is not a job queue name or job queue ARN "
        "(expected a name of up to 128 letters, digits, - and _, or "
        "arn:aws:batch:<region>:<12-digit account>:job-queue/<name>)"
    )
    with pytest.raises(ValueError, match=f"^{re.escape(want)}$"):
        RuleQueues({"r": "a b"})


def test_an_entry_for_a_rule_the_workflow_lacks_is_ignored():
    # Deliberate: one map can serve workflows that lack some of its rules.
    queues = RuleQueues({"not_a_rule": GPU, "align": CPU})
    assert queues.queue_of(_job("align")) == CPU
    assert queues.queue_of(_job("other")) is None


@pytest.mark.parametrize(
    "text, want",
    [
        ("[]", "a rule queue map must be a JSON object"),
        ('"q"', "a rule queue map must be a JSON object"),
        ('{"r": "a", "r": "b"}', "rule 'r' is in the map twice"),
        # A duplicate inside a nested value is not a rule given twice: the value is
        # what is wrong.
        (
            '{"r": {"x": 1, "x": 2}}',
            "rule queue map entry 'r': {'x': 2} is not a job queue name",
        ),
        ("[" * 100_000, "a rule queue map is nested too deeply"),
    ],
)
def test_maps_that_are_not_an_object_of_unique_rules_are_refused(text, want):
    with pytest.raises(ValueError, match=re.escape(want)):
        RuleQueues.from_json(text)


def test_the_setting_takes_inline_json():
    queues = RuleQueues.from_setting(f'  {{"align": "{GPU}"}}')
    assert queues.queue_of(_job("align")) == GPU
    assert RuleQueues.is_inline(f'  {{"align": "{GPU}"}}')


def test_a_missing_file_says_why_the_value_was_read_as_a_path():
    want = (
        "the value is read as a file path because it does not start with '{' (an "
        "inline map must be a JSON object)"
    )
    with pytest.raises(FileNotFoundError, match=re.escape(want)):
        RuleQueues.from_setting('["align", "q"]')


def test_the_setting_takes_a_file(tmp_path):
    path = tmp_path / "queues.json"
    path.write_text(json.dumps({"align": GPU}))
    queues = RuleQueues.from_setting(str(path))
    assert queues.queue_of(_job("align")) == GPU
    assert not RuleQueues.is_inline(str(path))


# ---------------------------------------------------------------------------
# Group jobs
# ---------------------------------------------------------------------------


def test_a_group_whose_rules_share_a_queue_gets_it():
    queues = RuleQueues({"a": GPU, "b": GPU})
    assert queues.queue_of(_group("a", "b", "a")) == GPU


def test_a_group_with_no_rule_in_the_map_gets_none():
    assert RuleQueues({"x": GPU}).queue_of(_group("a", "b")) is None


def test_a_group_whose_rules_map_to_different_queues_fails():
    group = _group("b", "a")
    want = (
        f"{group.name}: the rules of this group job do not share one queue in the rule "
        f"queue map (a -> {GPU}, b -> {CPU}). A group runs as one AWS Batch job, so "
        "map all of its rules to the same queue, or leave all of them out of the map"
    )
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}$"):
        RuleQueues({"a": GPU, "b": CPU}).queue_of(group)


def test_a_group_with_only_some_rules_in_the_map_fails():
    group = _group("a", "b")
    want = (
        f"{group.name}: the rules of this group job do not share one queue in the rule "
        f"queue map (a -> {GPU}, b -> (not in the map)). A group runs as one AWS Batch "
        "job, so map all of its rules to the same queue, or leave all of them out of "
        "the map"
    )
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}$"):
        RuleQueues({"a": GPU}).queue_of(group)


# ---------------------------------------------------------------------------
# Loading the map in Executor.__post_init__
# ---------------------------------------------------------------------------


def test_the_setting_is_an_executor_setting_read_from_its_env_var():
    (rule_queues,) = [
        f for f in dataclasses.fields(ExecutorSettings) if f.name == "rule_queues"
    ]
    assert rule_queues.default is None
    assert rule_queues.metadata["env_var"] is True


def _post_init(**settings) -> Executor:
    """Run Executor.__post_init__ with the given executor settings (AWS mocked)."""
    executor = object.__new__(Executor)
    executor.logger = MagicMock()
    executor.workflow = SimpleNamespace(
        remote_execution_settings=SimpleNamespace(container_image="global:1"),
        executor_settings=ExecutorSettings(region="us-east-1", **settings),
    )
    with patch("snakemake_executor_plugin_aws_batch.BatchClient"), patch.object(
        Executor, "_preflight_validate"
    ):
        executor.__post_init__()
    return executor


def test_no_map_is_loaded_without_the_setting():
    assert _post_init().rule_queues is None


@pytest.mark.parametrize(
    "mapping, count",
    [({"align": GPU}, "1 rule"), ({"align": GPU, "call": CPU}, "2 rules")],
)
def test_the_map_is_loaded_from_the_setting(mapping, count):
    executor = _post_init(rule_queues=json.dumps(mapping))
    assert executor.rule_queues.queue_of(_job("align")) == GPU
    executor.logger.info.assert_called_once_with(
        f"Rules are sent to their queues through the rule queue map ({count})"
    )


@pytest.mark.parametrize(
    "value, where, cause",
    [
        ("{not json", "", ValueError),
        ('{"r": "not a queue"}', "", ValueError),
        ("/does/not/exist.json", " /does/not/exist.json", OSError),
    ],
)
def test_a_map_that_cannot_be_read_fails_the_run(value, where, cause):
    want = f"Failed to read the rule queue map{where} (--aws-batch-rule-queues): "
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}") as err:
        _post_init(rule_queues=value)
    assert isinstance(err.value.__cause__, cause)


def test_a_map_file_that_is_not_json_is_named(tmp_path):
    path = tmp_path / "queues.json"
    path.write_text("{not json")
    want = f"Failed to read the rule queue map {path} (--aws-batch-rule-queues): "
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}"):
        _post_init(rule_queues=str(path))


# ---------------------------------------------------------------------------
# Choosing a job's queue
# ---------------------------------------------------------------------------


def _builder(job, rule_queue=None) -> BatchJobBuilder:
    return BatchJobBuilder(
        logger=MagicMock(),
        job=job,
        envvars={},
        container_image="img:1",
        settings=SimpleNamespace(job_queue="global-queue"),
        job_command="cmd",
        batch_client=MagicMock(),
        rule_queue=rule_queue,
    )


@pytest.mark.parametrize(
    "rule_queue, resources, expected",
    [
        # The map outranks the rule's batch_queue resource...
        (GPU, {"batch_queue": "rule-queue"}, GPU),
        (GPU, {}, GPU),
        # ...which outranks --aws-batch-job-queue.
        (None, {"batch_queue": "rule-queue"}, "rule-queue"),
        (None, {}, "global-queue"),
    ],
)
def test_the_job_queue_precedence(rule_queue, resources, expected):
    builder = _builder(_job("align", **resources), rule_queue=rule_queue)
    assert builder.job_queue == expected


def _executor_for_run_job(rule_queues=None) -> Executor:
    ex = Executor.__new__(Executor)
    ex.logger = MagicMock()
    ex.settings = SimpleNamespace(region="us-east-1", job_queue="global-queue")
    ex.batch_client = MagicMock()
    ex.container_image = "img:1"
    ex.rule_queues = rule_queues
    ex.envvars = MagicMock(return_value={})
    ex.format_job_exec = MagicMock(return_value="cmd")
    ex.report_job_submission = MagicMock()
    ex.report_job_error = MagicMock()
    ex._validate_queue = MagicMock()
    return ex


def _run_job(ex: Executor, job) -> MagicMock:
    """Run ``job`` and return the BatchJobBuilder class mock it was built with."""
    builder = MagicMock()
    builder.uses_preexisting_job_definition = False
    builder.submit.return_value = {"jobName": "n", "jobId": "j"}
    with patch(
        "snakemake_executor_plugin_aws_batch.BatchJobBuilder", return_value=builder
    ) as cls:
        ex.run_job(job)
    return cls


def test_run_job_without_a_map_passes_no_queue():
    ex = _executor_for_run_job()
    cls = _run_job(ex, _job("align"))
    assert cls.call_args.kwargs["rule_queue"] is None


def test_run_job_submits_to_the_mapped_queue():
    # A real builder and submit: the map's queue outranks the rule's batch_queue
    # resource in both the queue validation and the SubmitJob request.
    ex = _executor_for_run_job(RuleQueues({"align": GPU}))
    ex.batch_client.submit_job.return_value = {"jobName": "n", "jobId": "j"}
    with patch.object(
        BatchJobBuilder,
        "build_job_definition",
        return_value=({"jobDefinitionName": "def", "revision": 1}, "n"),
    ):
        ex.run_job(_job("align", batch_queue="rule-queue"))
    ex._validate_queue.assert_called_once_with(GPU)
    assert ex.batch_client.submit_job.call_args.kwargs["jobQueue"] == GPU


def test_the_builder_logs_when_the_map_overrides_batch_queue():
    builder = _builder(_job("align", batch_queue="rule-queue"), rule_queue=GPU)
    builder.logger.debug.assert_called_once_with(
        f"align: the rule queue map sends this job to {GPU}, not its batch_queue "
        "resource rule-queue"
    )


@pytest.mark.parametrize(
    "resources", [{}, {"batch_queue": GPU}], ids=["no batch_queue", "same queue"]
)
def test_the_builder_does_not_log_without_an_override(resources):
    builder = _builder(_job("align", **resources), rule_queue=GPU)
    builder.logger.debug.assert_not_called()


def test_a_group_job_split_across_queues_fails_without_being_submitted():
    ex = _executor_for_run_job(RuleQueues({"a": GPU, "b": CPU}))
    cls = _run_job(ex, _group("a", "b"))
    cls.assert_not_called()
    ex.report_job_submission.assert_not_called()
    ex.report_job_error.assert_called_once()
    assert "do not share one queue" in ex.report_job_error.call_args.kwargs["msg"]
