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
from typing import Any, Dict, List, Mapping, Optional, Tuple, Type, TypeVar

from snakemake_interface_common.exceptions import WorkflowError

DOCKER_HUB = "docker.io"
DOCKER_HUB_ALIASES = ("index.docker.io", "registry-1.docker.io")

_MapT = TypeVar("_MapT", bound="_ImageKeyedMap")


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


class _ImageKeyedMap:
    """Image references to values, each looked up as written or normalized.

    The base of :class:`ImageMap` (image to image) and
    :class:`~snakemake_executor_plugin_aws_batch.job_definition_map.JobDefinitionMap`
    (image to job definition). Subclasses set the class attributes below, which name
    the map in its errors, and :meth:`_check_entry`, which says what an entry may be.
    """

    # What the map is called in error messages, e.g. "job definition map".
    KIND: str
    # What an entry maps to, in the plural, e.g. "job definitions".
    VALUES: str
    # The error for a file that is not a JSON object.
    NOT_AN_OBJECT: str
    # The error for a job whose image is not in the map, formatted with the job's
    # name (``job_name``), its image (``image``) and where the map was read from
    # (``where``).
    MISSING: str

    def __init__(self, mapping: Mapping[str, str], path: Optional[str] = None):
        # Where the map was read from, for error messages.
        self.path = path
        self._entries: Dict[str, str] = {}
        for ref, value in mapping.items():
            self._check_entry(ref, value)
            self._entries[ref] = value
        # Each entry also answers to its normalized spelling, unless the map names that
        # spelling itself. Entries that share a normalized spelling must agree on the
        # value, or which one a job gets would depend on the order of the map.
        self._lookup: Dict[str, str] = dict(self._entries)
        named_by: Dict[str, str] = {}
        for ref, value in self._entries.items():
            key = normalize(ref)
            if key in self._entries:
                continue
            other = named_by.setdefault(key, ref)
            if self._lookup.setdefault(key, value) != value:
                raise ValueError(
                    f"{self.KIND} entries {other!r} and {ref!r} are both {key} but "
                    f"map to different {self.VALUES}; add an entry for {key!r} to "
                    "choose one"
                )

    def _check_entry(self, ref: Any, value: Any) -> None:
        """Raise ``ValueError`` when ``ref`` -> ``value`` is not a valid entry."""
        raise NotImplementedError

    @classmethod
    def from_json(cls: Type[_MapT], text: str, path: Optional[str] = None) -> _MapT:
        """Parse a JSON object of reference -> value; ``path`` is where it came from."""
        data = json.loads(text, object_pairs_hook=_unique_keys)
        if not isinstance(data, dict):
            raise ValueError(cls.NOT_AN_OBJECT)
        return cls(data, path=path)

    @classmethod
    def from_file(cls: Type[_MapT], path: str) -> _MapT:
        """Load a JSON object of reference -> value from ``path``."""
        return cls.from_json(Path(path).read_text(), path=path)

    def resolve(self, ref: str) -> Optional[str]:
        """The value for ``ref``, or ``None`` if the map does not have it."""
        return self._lookup.get(ref) or self._lookup.get(normalize(ref))

    def require(self, image: Any, job_name: str) -> str:
        """The value for ``job_name``'s ``image``. Raises ``WorkflowError`` when
        ``image`` is not a reference or not in the map."""
        if not _is_reference(image):
            raise WorkflowError(
                f"{job_name}: container image {image!r} is not an image reference"
            )
        value = self.resolve(image)
        if value is None:
            where = f" {self.path}" if self.path else ""
            raise WorkflowError(
                self.MISSING.format(job_name=job_name, image=image, where=where)
            )
        return value

    def __len__(self) -> int:
        """The number of entries in the map (not counting normalized spellings)."""
        return len(self._entries)


class ImageMap(_ImageKeyedMap):
    """Image references to the images to run in their place."""

    KIND = "image map"
    VALUES = "images"
    NOT_AN_OBJECT = "an image map must be a JSON object"
    MISSING = (
        "{job_name}: container image {image!r} is not in the container image "
        "map{where}; add it to the map, or map it to itself to run it unchanged"
    )

    def _check_entry(self, ref: Any, value: Any) -> None:
        if not _is_reference(ref) or not _is_reference(value):
            raise ValueError(
                f"image map entry {ref!r}: {value!r} is not a pair of references"
            )
