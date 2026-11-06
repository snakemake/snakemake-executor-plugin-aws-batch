import dataclasses
import json
import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_aws_batch import Executor, ExecutorSettings
from snakemake_executor_plugin_aws_batch.image_map import ImageMap, normalize

DIGEST = "sha256:" + "a" * 64
PINNED = f"123456789012.dkr.ecr.us-east-1.amazonaws.com/workers@{DIGEST}"


@pytest.mark.parametrize(
    "ref, want",
    [
        ("ubuntu", "docker.io/library/ubuntu:latest"),
        ("ubuntu:24.04", "docker.io/library/ubuntu:24.04"),
        ("  ubuntu:24.04\n", "docker.io/library/ubuntu:24.04"),
        ("biocontainers/samtools:1.21", "docker.io/biocontainers/samtools:1.21"),
        ("index.docker.io/library/ubuntu:24.04", "docker.io/library/ubuntu:24.04"),
        ("registry-1.docker.io/library/ubuntu", "docker.io/library/ubuntu:latest"),
        (
            "quay.io/biocontainers/fastqc:0.12.1--hdfd78af_0",
            "quay.io/biocontainers/fastqc:0.12.1--hdfd78af_0",
        ),
        ("localhost:5000/coord", "localhost:5000/coord:latest"),
        ("localhost/coord", "localhost/coord:latest"),
        ("Localhost/coord", "localhost/coord:latest"),
        ("Quay.IO/x/y:1", "quay.io/x/y:1"),
        (f"ubuntu@{DIGEST}", f"docker.io/library/ubuntu@{DIGEST}"),
        (f"quay.io/x/y@{DIGEST.replace('a', 'A')}", f"quay.io/x/y@{DIGEST}"),
        # A digest pins the image; the tag beside it does not change what is pulled.
        (f"quay.io/x/y:1@{DIGEST}", f"quay.io/x/y@{DIGEST}"),
    ],
)
def test_normalize_reads_references_like_docker(ref, want):
    assert normalize(ref) == want


def test_a_reference_resolves_as_written_or_normalized():
    image_map = ImageMap({"ubuntu:24.04": PINNED, PINNED: PINNED})
    assert image_map.resolve("ubuntu:24.04") == PINNED
    assert image_map.resolve("docker.io/library/ubuntu:24.04") == PINNED
    # An identity entry lets an already-pinned image through.
    assert image_map.resolve(PINNED) == PINNED
    assert image_map.resolve("ubuntu:22.04") is None


def test_a_digest_matches_with_or_without_a_tag():
    image_map = ImageMap({f"quay.io/x/y:1@{DIGEST}": PINNED})
    assert image_map.resolve(f"quay.io/x/y@{DIGEST}") == PINNED
    assert image_map.resolve(f"quay.io/x/y:2@{DIGEST}") == PINNED
    assert image_map.resolve("quay.io/x/y:1") is None


def test_a_spelling_the_map_names_wins_over_a_normalized_one():
    image_map = ImageMap({"ubuntu": "a", "docker.io/library/ubuntu:latest": "b"})
    assert image_map.resolve("ubuntu") == "a"
    assert image_map.resolve("docker.io/library/ubuntu:latest") == "b"
    # A third spelling resolves through the normalized one the map names.
    assert image_map.resolve("ubuntu:latest") == "b"


def test_spellings_of_one_image_must_agree():
    # Same image, same target: fine.
    image_map = ImageMap({"ubuntu": "a", "ubuntu:latest": "a"})
    assert image_map.resolve("docker.io/library/ubuntu") == "a"
    # Same image, different targets, and no entry for the normalized spelling.
    want = "image map entries 'ubuntu' and 'ubuntu:latest' are both "
    with pytest.raises(ValueError, match=re.escape(want)):
        ImageMap({"ubuntu": "a", "ubuntu:latest": "b"})


def test_len_counts_the_entries_in_the_map():
    assert len(ImageMap({"ubuntu": "a", "quay.io/x/y:1": "b"})) == 2


@pytest.mark.parametrize(
    "mapping", [{"x": ""}, {"x": "   "}, {"": "x"}, {"x": 1}, {"x": None}]
)
def test_an_entry_is_a_pair_of_references(mapping):
    with pytest.raises(ValueError, match="is not a pair of references"):
        ImageMap(mapping)


def test_the_map_is_read_from_a_json_object(tmp_path):
    path = tmp_path / "images.json"
    path.write_text(json.dumps({"quay.io/b/fastqc:1": PINNED}))
    image_map = ImageMap.from_file(str(path))
    assert image_map.resolve("quay.io/b/fastqc:1") == PINNED
    assert image_map.path == str(path)


def test_a_map_file_that_is_not_an_object_is_refused(tmp_path):
    path = tmp_path / "images.json"
    path.write_text("[]")
    with pytest.raises(ValueError, match="^an image map must be a JSON object$"):
        ImageMap.from_file(str(path))


def test_a_map_file_that_names_an_image_twice_is_refused(tmp_path):
    path = tmp_path / "images.json"
    path.write_text('{"ubuntu": "a", "ubuntu": "b"}')
    with pytest.raises(
        ValueError, match=re.escape("image 'ubuntu' is in the map twice")
    ):
        ImageMap.from_file(str(path))


def test_a_job_resolves_through_the_map_strictly(tmp_path):
    path = tmp_path / "images.json"
    path.write_text(json.dumps({"quay.io/b/fastqc:1": PINNED}))
    image_map = ImageMap.from_file(str(path))
    assert image_map.require("quay.io/b/fastqc:1", "rule a") == PINNED
    want = (
        f"rule b: container image 'quay.io/b/seqkit:2' is not in the container image "
        f"map {path}; add it to the map, or map it to itself to run it unchanged"
    )
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}$"):
        image_map.require("quay.io/b/seqkit:2", "rule b")


@pytest.mark.parametrize("image", [None, 3, ""])
def test_a_job_image_that_is_not_a_reference_is_refused(image):
    want = f"rule a: container image {image!r} is not an image reference"
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}$"):
        ImageMap({"ubuntu": "a"}).require(image, "rule a")


# ---------------------------------------------------------------------------
# Loading the map in Executor.__post_init__
# ---------------------------------------------------------------------------


def test_the_setting_is_an_executor_setting():
    names = {f.name for f in dataclasses.fields(ExecutorSettings)}
    assert "container_image_map" in names


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
    assert _post_init().image_map is None


def test_the_map_is_loaded_from_the_setting(tmp_path):
    path = tmp_path / "images.json"
    path.write_text(json.dumps({"global:1": PINNED}))
    executor = _post_init(container_image_map=str(path))
    assert executor.image_map.resolve("global:1") == PINNED
    executor.logger.info.assert_called_once_with(
        f"Container images are resolved through {path} (1 entries)"
    )


@pytest.mark.parametrize(
    "content, cause",
    [(None, OSError), ("{not json", ValueError), ("[]", ValueError)],
)
def test_a_map_that_cannot_be_read_fails_the_run(tmp_path, content, cause):
    path = tmp_path / "images.json"
    if content is not None:
        path.write_text(content)
    want = f"Failed to read the container image map {path}: "
    with pytest.raises(WorkflowError, match=f"^{re.escape(want)}") as err:
        _post_init(container_image_map=str(path))
    assert isinstance(err.value.__cause__, cause)


def test_the_map_cannot_be_combined_with_a_pre_existing_job_definition(tmp_path):
    path = tmp_path / "images.json"
    path.write_text(json.dumps({"global:1": PINNED}))
    with pytest.raises(WorkflowError, match="Cannot combine the container image map"):
        _post_init(container_image_map=str(path), job_definition="my-def:3")
