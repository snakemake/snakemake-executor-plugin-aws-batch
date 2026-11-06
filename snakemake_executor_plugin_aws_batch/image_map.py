"""A container image map: the images a workflow may run, mapped to the images to run.

A deployment that mirrors or rebuilds every image a workflow declares (e.g. into a
private registry, pinned by digest) passes the map with
``--aws-batch-container-image-map`` (or ``SNAKEMAKE_AWS_BATCH_CONTAINER_IMAGE_MAP``).
Every job's image, the global ``--container-image`` or a rule's
``aws_batch_container_image`` resource, is then looked up in it, and a job whose image
is not in the map fails instead of pulling an image the deployment did not vet.

The map is a JSON object of image reference to image reference. A reference is looked up
as written, then in its normalized form (``ubuntu`` and
``docker.io/library/ubuntu:latest`` are the same image), so the map may use either
spelling.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from snakemake_interface_common.exceptions import WorkflowError

DOCKER_HUB = "docker.io"
DOCKER_HUB_ALIASES = ("index.docker.io", "registry-1.docker.io")


def normalize(ref: str) -> str:
    """The canonical spelling of an image reference, as Docker reads it.

    The first path component is a registry only when it contains a ``.`` or ``:``, is
    ``localhost``, or has an uppercase letter; registry names are case-insensitive, so
    it is lower-cased. Otherwise the image is on Docker Hub, and a one-part name is
    under ``library/``. A digest pins the image, so a tag beside a digest is dropped
    (Docker pulls ``name:tag@digest`` by its digest). A reference with neither tag nor
    digest means ``:latest``.
    """
    ref = ref.strip()
    name, _, digest = ref.partition("@")
    tag = None
    if ":" in name.rsplit("/", 1)[-1]:
        name, tag = name.rsplit(":", 1)
    first, _, rest = name.partition("/")
    if rest and (
        "." in first or ":" in first or first == "localhost" or first != first.lower()
    ):
        registry, repository = first.lower(), rest
    else:
        registry, repository = DOCKER_HUB, name
    if registry in DOCKER_HUB_ALIASES:
        registry = DOCKER_HUB
    if registry == DOCKER_HUB and "/" not in repository:
        repository = f"library/{repository}"
    if digest:
        return f"{registry}/{repository}@{digest.lower()}"
    return f"{registry}/{repository}:{tag or 'latest'}"


def _is_reference(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _unique_keys(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    """A JSON object hook that rejects a key given twice (JSON keeps the last)."""
    obj: Dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError(f"image {key!r} is in the map twice")
        obj[key] = value
    return obj


class ImageMap:
    """Image references to the images to run in their place."""

    def __init__(self, mapping: Mapping[str, str], path: Optional[str] = None):
        # Where the map was read from, for error messages.
        self.path = path
        self._entries: Dict[str, str] = {}
        for ref, image in mapping.items():
            if not _is_reference(ref) or not _is_reference(image):
                raise ValueError(
                    f"image map entry {ref!r}: {image!r} is not a pair of references"
                )
            self._entries[ref] = image
        # Each entry also answers to its normalized spelling, unless the map names that
        # spelling itself. Entries that share a normalized spelling must agree on the
        # image, or which one a job gets would depend on the order of the map.
        self._lookup: Dict[str, str] = dict(self._entries)
        named_by: Dict[str, str] = {}
        for ref, image in self._entries.items():
            key = normalize(ref)
            if key in self._entries:
                continue
            other = named_by.setdefault(key, ref)
            if self._lookup.setdefault(key, image) != image:
                raise ValueError(
                    f"image map entries {other!r} and {ref!r} are both {key} but map "
                    f"to different images; add an entry for {key!r} to choose one"
                )

    @classmethod
    def from_file(cls, path: str) -> "ImageMap":
        """Load a JSON object of reference -> reference."""
        data = json.loads(Path(path).read_text(), object_pairs_hook=_unique_keys)
        if not isinstance(data, dict):
            raise ValueError("an image map must be a JSON object")
        return cls(data, path=path)

    def resolve(self, ref: str) -> Optional[str]:
        """The image to run for ``ref``, or ``None`` if the map does not have it."""
        return self._lookup.get(ref) or self._lookup.get(normalize(ref))

    def require(self, ref: Any, job_name: str) -> str:
        """The image to run ``job_name`` with in place of ``ref``. Raises
        ``WorkflowError`` when ``ref`` is not a reference or not in the map."""
        if not _is_reference(ref):
            raise WorkflowError(
                f"{job_name}: container image {ref!r} is not an image reference"
            )
        image = self.resolve(ref)
        if image is None:
            where = f" {self.path}" if self.path else ""
            raise WorkflowError(
                f"{job_name}: container image {ref!r} is not in the container image "
                f"map{where}; add it to the map, or map it to itself to run it "
                "unchanged"
            )
        return image

    def __len__(self) -> int:
        """The number of entries in the map (not counting normalized spellings)."""
        return len(self._entries)
