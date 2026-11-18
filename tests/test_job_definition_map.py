"""Unit tests for the job definition map (--aws-batch-job-definition-map).

All tests run with mocked AWS clients; no AWS credentials are required.
"""

import dataclasses
import json
import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_aws_batch import Executor, ExecutorSettings
from snakemake_executor_plugin_aws_batch.batch_client import BatchClient
from snakemake_executor_plugin_aws_batch.image_map import ImageMap
from snakemake_executor_plugin_aws_batch.job_definition_map import (
    JobDefinitionMap,
    describe_job_definition,
)

DEF_ARN = "arn:aws:batch:us-east-1:123456789012:job-definition/samtools:4"


@pytest.fixture(autouse=True)
def _clear_batch_job_tags(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep mapping tests independent of the developer's Batch tag settings."""
    monkeypatch.delenv("SNAKEMAKE_AWS_BATCH_JOB_TAGS", raising=False)


# ---------------------------------------------------------------------------
# JobDefinitionMap
# ---------------------------------------------------------------------------


def test_an_image_resolves_as_written_or_normalized():
    definitions = JobDefinitionMap({"ubuntu:24.04": "ubuntu-def:2"})
    assert definitions.require("ubuntu:24.04", "rule a") == "ubuntu-def:2"
    assert (
        definitions.require("docker.io/library/ubuntu:24.04", "rule a")
        == "ubuntu-def:2"
    )


def test_an_image_missing_from_the_map_is_refused():
    definitions = JobDefinitionMap({"ubuntu:24.04": "ubuntu-def:2"}, path="defs.json")
    want = (
        "rule a: container image 'ubuntu:22.04' has no entry in the job definition "
        "map defs.json; register a job definition for it and add it to the map"
    )
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}$"):
        definitions.require("ubuntu:22.04", "rule a")


@pytest.mark.parametrize("image", [None, 3, ""])
def test_a_job_image_that_is_not_a_reference_is_refused(image):
    want = f"rule a: container image {image!r} is not an image reference"
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}$"):
        JobDefinitionMap({"ubuntu": "def"}).require(image, "rule a")


@pytest.mark.parametrize(
    "mapping, want",
    [
        ({"": "def"}, "job definition map entry '': the key is not an image reference"),
        (
            {"x": 1},
            "job definition map entry 'x': 1 is not a name, a name:revision pair, or "
            "a job definition ARN with a revision",
        ),
        (
            {"x": ""},
            "job definition map entry 'x': '' is not a name, a name:revision pair, "
            "or a job definition ARN with a revision",
        ),
    ],
)
def test_an_invalid_entry_names_the_invalid_side(mapping, want):
    with pytest.raises(ValueError, match=f"^{re.escape(want)}$"):
        JobDefinitionMap(mapping)


@pytest.mark.parametrize(
    "job_definition",
    [
        " my-def",
        "my-def:3 ",
        "my def",
        "my-def:latest",
        "my-def:3:4",
        "my-def:0",
        "my-def:03",
        "x" * 129,
        "arn:aws:batch:us-east-1:123456789012:job-queue/q",
        "arn:aws:iam::123456789012:role/r",
        "arn:aws:batch",
        "arn:aws:ecs:us-east-1:123456789012:job-definition/x:1",
        "arn:aws:batch::123456789012:job-definition/x:1",
        "arn:aws:batch:us-east-1::job-definition/x:1",
        "arn:aws:batch:us-east-1:123456789012:job-definition/x:0",
        "arn::batch:us-east-1:123456789012:job-definition/x:1",
        "arn:aws:batch:us-east-1:12345678901:job-definition/x:1",
        "arn:aws:batch:us-east-1:1234567890123:job-definition/x:1",
        "arn:aws:batch:us-east-1:12345678901x:job-definition/x:1",
        "arn:aws:batch:us-east-1:" + chr(0x661) * 12 + ":job-definition/x:1",
    ],
)
def test_a_value_that_is_not_a_job_definition_is_refused(job_definition):
    with pytest.raises(ValueError, match="^job definition map entry 'ubuntu': "):
        JobDefinitionMap({"ubuntu": job_definition})


@pytest.mark.parametrize(
    "job_definition",
    [
        "my_def-1",
        "my-def:3",
        "x" * 128,
        DEF_ARN,
        "arn:aws-us-gov:batch:us-gov-west-1:123456789012:job-definition/x:1",
        "arn:aws-cn:batch:cn-north-1:123456789012:job-definition/x:1",
    ],
)
def test_every_accepted_form_resolves(job_definition):
    assert JobDefinitionMap({"ubuntu": job_definition}).resolve("ubuntu") == (
        job_definition
    )


def test_an_arn_without_a_revision_is_refused():
    arn = "arn:aws:batch:us-east-1:123456789012:job-definition/samtools"
    want = (
        f"job definition map entry 'ubuntu': {arn!r} has no revision: a job "
        "definition ARN must end in ':<revision>' (or give the bare name to use its "
        "latest revision)"
    )
    with pytest.raises(ValueError, match=f"^{re.escape(want)}$"):
        JobDefinitionMap({"ubuntu": arn})


def test_spellings_of_one_image_must_agree_on_the_definition():
    want = (
        "job definition map entries 'ubuntu' and 'ubuntu:latest' are both "
        "docker.io/library/ubuntu:latest but map to different job definitions"
    )
    with pytest.raises(ValueError, match=re.escape(want)):
        JobDefinitionMap({"ubuntu": "a", "ubuntu:latest": "b"})


def test_the_map_is_read_inline_or_from_a_file(tmp_path):
    inline = JobDefinitionMap.from_setting(' {"ubuntu": "def:1"}')
    assert inline.require("ubuntu", "rule a") == "def:1"
    assert inline.path is None

    path = tmp_path / "defs.json"
    path.write_text(json.dumps({"ubuntu": DEF_ARN}))
    from_file = JobDefinitionMap.from_setting(str(path))
    assert from_file.require("ubuntu", "rule a") == DEF_ARN
    assert from_file.path == str(path)


@pytest.mark.parametrize("text", ["[]", '"ubuntu"'])
def test_a_map_that_is_not_an_object_is_refused(tmp_path, text):
    path = tmp_path / "defs.json"
    path.write_text(text)
    with pytest.raises(
        ValueError, match="^a job definition map must be a JSON object$"
    ):
        JobDefinitionMap.from_setting(str(path))


@pytest.mark.parametrize(
    "job_definition, want",
    [
        ("my-def", {"jobDefinitionName": "my-def", "status": "ACTIVE"}),
        ("my-def:3", {"jobDefinitions": ["my-def:3"]}),
        (DEF_ARN, {"jobDefinitions": [DEF_ARN]}),
    ],
)
def test_describe_follows_the_maps_forms(job_definition, want):
    client = MagicMock()
    client.describe_job_definitions.return_value = [{"revision": 3}]
    assert describe_job_definition(client, job_definition) == {"revision": 3}
    client.describe_job_definitions.assert_called_once_with(**want)


def test_describe_returns_the_highest_revision():
    client = MagicMock()
    client.describe_job_definitions.return_value = [
        {"revision": 2},
        {"revision": 3},
        {"revision": 1},
    ]
    assert describe_job_definition(client, "my-def") == {"revision": 3}


def test_describe_returns_none_when_there_is_no_definition():
    client = MagicMock()
    client.describe_job_definitions.return_value = []
    assert describe_job_definition(client, "my-def") is None


def test_the_batch_client_reads_every_page_of_definitions():
    with patch("snakemake_executor_plugin_aws_batch.batch_client.boto3"):
        client = BatchClient(region_name="us-east-1")
    paginator = client.client.get_paginator.return_value
    paginator.paginate.return_value = [
        {"jobDefinitions": [{"revision": 2}]},
        {"jobDefinitions": [{"revision": 3}, {"revision": 1}]},
        {},
    ]
    request = {"jobDefinitionName": "my-def", "status": "ACTIVE"}
    assert client.describe_job_definitions(**request) == [
        {"revision": 2},
        {"revision": 3},
        {"revision": 1},
    ]
    client.client.get_paginator.assert_called_once_with("describe_job_definitions")
    paginator.paginate.assert_called_once_with(**request)


@pytest.mark.parametrize(
    "requested, want", [("us-east-1", "us-east-1"), (None, "eu-west-1")]
)
def test_the_batch_client_records_the_region_it_uses(requested, want):
    with patch("snakemake_executor_plugin_aws_batch.batch_client.boto3") as boto3:
        boto3.client.return_value.meta.region_name = "eu-west-1"
        client = BatchClient(region_name=requested)
    assert client.region_name == want


# ---------------------------------------------------------------------------
# Loading the map in Executor.__post_init__
# ---------------------------------------------------------------------------


def test_the_setting_is_an_executor_setting_with_an_env_var():
    fields = {f.name: f for f in dataclasses.fields(ExecutorSettings)}
    assert fields["job_definition_map"].metadata["env_var"] is True


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
    assert _post_init().job_definition_map is None


def test_the_map_is_loaded_from_an_inline_setting():
    executor = _post_init(job_definition_map='{"global:1": "def:1"}')
    assert executor.job_definition_map.require("global:1", "rule a") == "def:1"
    executor.logger.info.assert_called_once_with(
        "Jobs run on the pre-registered job definitions in the inline job "
        "definition map (1 entries)"
    )


def test_the_map_is_loaded_from_a_file_setting(tmp_path):
    path = tmp_path / "defs.json"
    path.write_text(json.dumps({"global:1": "def:1"}))
    executor = _post_init(job_definition_map=str(path))
    assert executor.job_definition_map.require("global:1", "rule a") == "def:1"
    executor.logger.info.assert_called_once_with(
        f"Jobs run on the pre-registered job definitions in {path} (1 entries)"
    )


@pytest.mark.parametrize(
    "value, source, cause",
    [
        ("{not json", "given inline", ValueError),
        ("/does/not/exist.json", "/does/not/exist.json", OSError),
    ],
)
def test_a_map_that_cannot_be_read_fails_the_run(value, source, cause):
    want = f"Failed to read the job definition map ({source}): "
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}") as err:
        _post_init(job_definition_map=value)
    assert isinstance(err.value.__cause__, cause)
    hint = "(a value that does not start with '{' is read as the path of a file)"
    assert str(err.value).endswith(hint) == (cause is OSError)


def test_a_malformed_job_definition_fails_the_run():
    want = "Failed to read the job definition map (given inline): job definition "
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}"):
        _post_init(job_definition_map='{"global:1": "def:1 "}')


@pytest.mark.parametrize(
    "setting, option",
    [
        ({"job_definition": "my-def:3"}, "--aws-batch-job-definition"),
        (
            {"job_role": "arn:aws:iam::1:role/r"},
            "--aws-batch-job-role (or SNAKEMAKE_AWS_BATCH_JOB_ROLE)",
        ),
        ({"log_group": "/snakemake/x"}, "--aws-batch-log-group"),
        (
            {"job_role": "arn:aws:iam::1:role/r", "log_group": "/snakemake/x"},
            "--aws-batch-job-role (or SNAKEMAKE_AWS_BATCH_JOB_ROLE), "
            "--aws-batch-log-group",
        ),
    ],
)
def test_the_map_refuses_settings_the_definitions_own(setting, option):
    want = (
        "Cannot combine the job definition map (--aws-batch-job-definition-map or "
        f"SNAKEMAKE_AWS_BATCH_JOB_DEFINITION_MAP) with {option}: "
    )
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}") as err:
        _post_init(job_definition_map='{"global:1": "def:1"}', **setting)
    assert str(err.value).endswith(
        "; remove the option or unset the environment variable"
    )


def test_the_map_combines_with_the_container_image_map(tmp_path):
    path = tmp_path / "images.json"
    path.write_text(json.dumps({"global:1": "pinned:1"}))
    executor = _post_init(
        container_image_map=str(path), job_definition_map='{"pinned:1": "def:1"}'
    )
    image = executor.image_map.require("global:1", "rule a")
    assert executor.job_definition_map.require(image, "rule a") == "def:1"


# ---------------------------------------------------------------------------
# Executor.run_job in map mode (a real BatchJobBuilder; the Batch client mocked)
# ---------------------------------------------------------------------------


def _executor(definitions: dict, image_map=None, describe=None) -> Executor:
    """An Executor in job definition map mode, without running __post_init__.

    ``describe`` maps a job definition, as the describe_job_definitions request
    names it (``jobDefinitionName`` or the one ``jobDefinitions`` entry), to the
    definitions the Batch client returns for it.
    """
    executor = object.__new__(Executor)
    executor.logger = MagicMock()
    executor.batch_client = MagicMock()
    executor.batch_client.submit_job.return_value = {
        "jobName": "snakejob-rule_a",
        "jobId": "abc-123",
    }

    def describe_job_definitions(**request):
        key = request.get("jobDefinitionName") or request["jobDefinitions"][0]
        return list((describe or {}).get(key, []))

    executor.batch_client.describe_job_definitions.side_effect = (
        describe_job_definitions
    )
    executor.batch_client.region_name = "us-east-1"
    executor.settings = SimpleNamespace(
        region="us-east-1",
        job_queue="test-queue",
        job_role=None,
        job_definition=None,
        log_group=None,
        tags=None,
        task_timeout=None,
    )
    executor.container_image = "global:1"
    executor.image_map = image_map
    executor.job_definition_map = JobDefinitionMap(definitions)
    executor._mapped_job_definition_cache = {}
    executor.envvars = MagicMock(return_value={"A": "1"})
    executor.format_job_exec = MagicMock(return_value="snakemake ...")
    executor.report_job_submission = MagicMock()
    executor.report_job_error = MagicMock()
    executor._validate_queue = MagicMock()
    return executor


def _job(name: str = "rule_a", **resources) -> MagicMock:
    job = MagicMock()
    job.name = name
    job.threads = 2
    job.resources = {"_cores": 2, "mem_mb": 4096, **resources}
    return job


def _definition(
    revision: int, shm_mb=None, platform: str = "EC2", status: str = "ACTIVE"
) -> dict:
    properties: dict = {"image": "global:1"}
    if shm_mb is not None:
        properties["linuxParameters"] = {"sharedMemorySize": shm_mb}
    return {
        "revision": revision,
        "status": status,
        "containerProperties": properties,
        "platformCapabilities": [platform],
    }


def _described(executor: Executor) -> list:
    """The describe_job_definitions requests the executor made."""
    return [
        c.kwargs for c in executor.batch_client.describe_job_definitions.call_args_list
    ]


def _error(executor: Executor) -> str:
    executor.report_job_submission.assert_not_called()
    executor.batch_client.submit_job.assert_not_called()
    executor.report_job_error.assert_called_once()
    return executor.report_job_error.call_args.kwargs["msg"]


def test_a_job_is_submitted_on_its_images_definition_without_registering():
    executor = _executor({"global:1": "global-def:1", "rule:2": "rule-def:5"})
    executor.run_job(_job())
    executor.run_job(_job("rule_b", aws_batch_container_image="rule:2"))

    submitted = [c.kwargs for c in executor.batch_client.submit_job.call_args_list]
    assert [s["jobDefinition"] for s in submitted] == ["global-def:1", "rule-def:5"]
    overrides = submitted[0]["containerOverrides"]
    assert overrides["command"] == ["/bin/bash", "-c", "snakemake ..."]
    assert overrides["environment"] == [{"name": "A", "value": "1"}]
    assert overrides["resourceRequirements"] == [
        {"type": "VCPU", "value": "2"},
        {"type": "MEMORY", "value": "4096"},
    ]
    executor.batch_client.register_job_definition.assert_not_called()
    # A pre-existing definition skips the queue check (and its Describe* calls).
    executor._validate_queue.assert_not_called()
    executor.report_job_error.assert_not_called()


def test_a_mapped_definition_is_never_deregistered():
    executor = _executor({"global:1": "global-def:1"})
    executor.run_job(_job())
    job_info = executor.report_job_submission.call_args.args[0]
    job_info.aux["job_definition_arn"] = "arn:...:job-definition/global-def:1"
    executor.cleanup_job_resources(job_info)
    executor.batch_client.deregister_job_definition.assert_not_called()


def test_the_definition_is_looked_up_by_the_image_the_image_map_resolves():
    executor = _executor(
        {"pinned@sha256:1": "pinned-def:1"},
        image_map=ImageMap({"global:1": "pinned@sha256:1"}),
    )
    executor.run_job(_job())
    submitted = executor.batch_client.submit_job.call_args.kwargs
    assert submitted["jobDefinition"] == "pinned-def:1"


def test_an_image_missing_from_the_map_fails_the_job():
    executor = _executor({"global:1": "global-def:1"})
    executor.run_job(_job(aws_batch_container_image="other:1"))
    _error(executor)
    executor.batch_client.register_job_definition.assert_not_called()


def test_a_per_rule_job_definition_fails_the_job():
    executor = _executor({"global:1": "global-def:1"})
    executor.run_job(_job(aws_batch_job_definition="privileged-def:1"))
    assert _error(executor).startswith(
        "rule_a: cannot run on the job definition 'privileged-def:1' "
        "(aws_batch_job_definition) with a job definition map: the map chooses each "
        "job's definition by its image; remove the aws_batch_job_definition resource "
        "from the rule"
    )


def test_shared_memory_up_to_the_definitions_size_is_accepted():
    executor = _executor(
        {"global:1": "global-def:1"},
        describe={"global-def:1": [_definition(1, shm_mb=8192)]},
    )
    executor.run_job(_job(shared_memory_size_mb=8192))
    executor.run_job(_job("rule_b", shared_memory_size_mb="4096"))

    executor.report_job_error.assert_not_called()
    assert executor.batch_client.submit_job.call_count == 2
    executor.batch_client.register_job_definition.assert_not_called()
    # Described once and cached.
    assert _described(executor) == [{"jobDefinitions": ["global-def:1"]}]


def test_each_definition_is_checked_against_its_own_shared_memory():
    executor = _executor(
        {"global:1": "big-def:1", "small:1": "small-def:1"},
        describe={
            "big-def:1": [_definition(1, shm_mb=8192)],
            "small-def:1": [_definition(1, shm_mb=1024)],
        },
    )
    executor.run_job(_job(shared_memory_size_mb=2048))
    executor.report_job_error.assert_not_called()
    executor.run_job(
        _job("rule_b", aws_batch_container_image="small:1", shared_memory_size_mb=2048)
    )
    assert "exceeds the 1024 MiB" in executor.report_job_error.call_args.kwargs["msg"]
    assert _described(executor) == [
        {"jobDefinitions": ["big-def:1"]},
        {"jobDefinitions": ["small-def:1"]},
    ]


def test_shared_memory_above_the_definitions_size_fails_the_job():
    executor = _executor(
        {"global:1": "global-def:1"},
        describe={"global-def:1": [_definition(1, shm_mb=1024)]},
    )
    executor.run_job(_job(shared_memory_size_mb=2048))
    want = (
        "rule_a: shared_memory_size_mb=2048 exceeds the 1024 MiB of shared memory "
        "(linuxParameters.sharedMemorySize) of job definition 'global-def:1'"
    )
    assert _error(executor).startswith(want)


def test_an_ec2_definition_without_shared_memory_refuses_any():
    executor = _executor(
        {"global:1": "global-def:1"}, describe={"global-def:1": [_definition(1)]}
    )
    executor.run_job(_job(shared_memory_size_mb=64))
    assert _error(executor) == (
        "rule_a: shared_memory_size_mb=64 cannot be met: job definition "
        "'global-def:1', which the job definition map names for 'global:1', sets no "
        "shared memory size (linuxParameters.sharedMemorySize); drop the resource or "
        "register the definition with one"
    )


@pytest.mark.parametrize("shm_mb", [None, 64])
def test_shared_memory_is_ignored_with_a_warning_on_a_fargate_definition(shm_mb):
    executor = _executor(
        {"global:1": "global-def:1"},
        describe={"global-def:1": [_definition(1, shm_mb, platform="FARGATE")]},
    )
    executor.run_job(_job(shared_memory_size_mb=2048))
    executor.report_job_error.assert_not_called()
    submitted = executor.batch_client.submit_job.call_args.kwargs
    assert submitted["jobDefinition"] == "global-def:1"
    executor.logger.warning.assert_called_once_with(
        "rule_a: shared_memory_size_mb=2048 is ignored on the Fargate job definition "
        "'global-def:1', which the job definition map names for 'global:1': Fargate "
        "cannot set /dev/shm (linuxParameters.sharedMemorySize)"
    )


@pytest.mark.parametrize(
    "properties", ["ecsProperties", "nodeProperties", "eksProperties"]
)
@pytest.mark.parametrize("platform", ["EC2", "FARGATE"])
def test_a_definition_without_container_properties_fails_the_job(
    properties: str, platform: str
) -> None:
    definition = {
        "revision": 1,
        "status": "ACTIVE",
        properties: {},
        "platformCapabilities": [platform],
    }
    executor = _executor(
        {"global:1": "global-def:1"}, describe={"global-def:1": [definition]}
    )
    executor.run_job(_job(shared_memory_size_mb=64))
    assert _error(executor) == (
        "rule_a: shared_memory_size_mb=64 cannot be checked against job definition "
        "'global-def:1', which the job definition map names for 'global:1': it has "
        "no containerProperties (it uses ECS, multi-node or EKS properties), so the "
        "shared memory check does not apply to it; drop the resource or map the "
        "image to a container job definition"
    )


def test_an_inactive_definition_fails_the_job():
    executor = _executor(
        {"global:1": "global-def:1"},
        describe={"global-def:1": [_definition(1, 8192, status="INACTIVE")]},
    )
    executor.run_job(_job(shared_memory_size_mb=64))
    assert _error(executor) == (
        "rule_a: job definition 'global-def:1', which the job definition map names "
        "for this job's image, is INACTIVE (not ACTIVE), so jobs cannot be submitted "
        "to it; map the image to an ACTIVE revision"
    )


def test_jobs_without_shared_memory_never_describe_the_definition():
    executor = _executor({"global:1": "global-def:1"})
    executor.run_job(_job())
    executor.batch_client.describe_job_definitions.assert_not_called()


def test_a_bare_name_is_checked_against_its_latest_active_revision():
    # The highest revision comes first, so taking the last one returned would pass.
    executor = _executor(
        {"global:1": "global-def"},
        describe={"global-def": [_definition(2, 1024), _definition(1, shm_mb=8192)]},
    )
    executor.run_job(_job(shared_memory_size_mb=2048))
    assert "exceeds the 1024 MiB" in _error(executor)


def test_a_bare_names_revision_is_described_once_per_run():
    # A revision registered after the first describe is not seen for the run.
    describe = {"global-def": [_definition(3, 4096)]}
    executor = _executor({"global:1": "global-def"}, describe=describe)
    executor.run_job(_job(shared_memory_size_mb=2048))
    describe["global-def"] = [_definition(4, 64)]
    executor.run_job(_job("rule_b", shared_memory_size_mb=2048))
    executor.report_job_error.assert_not_called()
    assert executor.batch_client.describe_job_definitions.call_count == 1
    # Both jobs run on the revision that was checked, not whatever is latest when
    # each is submitted.
    submitted = [
        c.kwargs["jobDefinition"]
        for c in executor.batch_client.submit_job.call_args_list
    ]
    assert submitted == ["global-def:3", "global-def:3"]


def test_a_rejected_cached_bare_revision_is_rediscovered_for_the_next_job() -> None:
    describe = {"global-def": [_definition(3, 4096)]}
    executor = _executor({"global:1": "global-def"}, describe=describe)
    executor.run_job(_job(shared_memory_size_mb=2048))
    executor.batch_client.submit_job.side_effect = ClientError(
        {"Error": {"Code": "ClientException", "Message": "Job definition is INACTIVE"}},
        "SubmitJob",
    )
    failed_job = _job("rule_b", shared_memory_size_mb=2048)
    executor.run_job(failed_job)
    executor.report_job_error.assert_called_once()
    (failed_info,) = executor.report_job_error.call_args.args
    assert failed_info.job is failed_job
    assert "Job definition is INACTIVE" in (
        executor.report_job_error.call_args.kwargs["msg"]
    )
    describe["global-def"] = [_definition(4, 4096)]
    executor.batch_client.submit_job.side_effect = None
    executor.run_job(_job("rule_c", shared_memory_size_mb=2048))
    assert _described(executor) == [
        {"jobDefinitionName": "global-def", "status": "ACTIVE"},
        {"jobDefinitionName": "global-def", "status": "ACTIVE"},
    ]
    assert [
        call.kwargs["jobDefinition"]
        for call in executor.batch_client.submit_job.call_args_list
    ] == ["global-def:3", "global-def:3", "global-def:4"]
    assert executor.report_job_submission.call_count == 2
    executor.report_job_error.assert_called_once()


@pytest.mark.parametrize("mapped", ["global-def:3", DEF_ARN])
def test_a_rejected_pinned_revision_is_not_rediscovered(mapped: str) -> None:
    executor = _executor(
        {"global:1": mapped}, describe={mapped: [_definition(3, 4096)]}
    )
    executor.batch_client.submit_job.side_effect = ClientError(
        {"Error": {"Code": "ClientException", "Message": "Job definition is INACTIVE"}},
        "SubmitJob",
    )
    for _ in range(2):
        with pytest.raises(WorkflowError, match="Job definition is INACTIVE"):
            executor.run_job(_job(shared_memory_size_mb=2048))
    assert _described(executor) == [{"jobDefinitions": [mapped]}]
    assert [
        call.kwargs["jobDefinition"]
        for call in executor.batch_client.submit_job.call_args_list
    ] == [mapped, mapped]


def test_a_bare_name_without_shared_memory_is_submitted_as_mapped():
    # Nothing is checked, so nothing is described, and SubmitJob resolves the name.
    executor = _executor({"global:1": "global-def"})
    executor.run_job(_job())
    executor.batch_client.describe_job_definitions.assert_not_called()
    (call,) = executor.batch_client.submit_job.call_args_list
    assert call.kwargs["jobDefinition"] == "global-def"


@pytest.mark.parametrize(
    "mapped",
    [
        "global-def:2",
        "arn:aws:batch:eu-west-1:123456789012:job-definition/global-def:2",
    ],
)
def test_a_mapped_revision_is_submitted_as_mapped(mapped):
    describe = {mapped: [_definition(2, 4096)]}
    executor = _executor({"global:1": mapped}, describe=describe)
    executor.run_job(_job(shared_memory_size_mb=2048))
    executor.report_job_error.assert_not_called()
    (call,) = executor.batch_client.submit_job.call_args_list
    assert call.kwargs["jobDefinition"] == mapped


def test_a_definition_that_cannot_be_described_fails_the_job_and_is_retried():
    executor = _executor({"global:1": "global-def:1"})
    describe = executor.batch_client.describe_job_definitions
    describe.side_effect = RuntimeError("denied")
    executor.run_job(_job(shared_memory_size_mb=64))
    assert "needs batch:DescribeJobDefinitions" in _error(executor)
    executor.run_job(_job(shared_memory_size_mb=64))
    assert describe.call_count == 2


def test_a_definition_that_does_not_exist_fails_the_job():
    executor = _executor({"global:1": "global-def:1"})
    executor.run_job(_job(shared_memory_size_mb=64))
    assert _error(executor) == (
        "rule_a: job definition 'global-def:1', which the job definition map names "
        "for this job's image, was not found; check the map entry and that the "
        "definition is ACTIVE in region us-east-1"
    )


@pytest.mark.parametrize("region", ["us-east-1", "eu-west-1"])
def test_a_missing_arn_reports_the_submission_and_arn_regions(region: str) -> None:
    arn = f"arn:aws:batch:{region}:123456789012:job-definition/samtools:4"
    executor = _executor({"global:1": arn})
    executor.run_job(_job(shared_memory_size_mb=64))
    message = _error(executor)
    assert "submission region us-east-1" in message
    assert f"ARN region {region}" in message
    assert "use a definition in the submission region" in message


@pytest.mark.parametrize("value", ["4g", "0", -1])
def test_an_invalid_shared_memory_resource_stops_the_run(value):
    # As on the dynamic path, where the builder raises it out of run_job.
    executor = _executor({"global:1": "global-def:1"})
    with pytest.raises(WorkflowError, match="^Invalid shared_memory_size_mb resource"):
        executor.run_job(_job(shared_memory_size_mb=value))
    executor.report_job_error.assert_not_called()
    executor.batch_client.submit_job.assert_not_called()
