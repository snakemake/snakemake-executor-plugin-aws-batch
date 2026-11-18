"""A job definition map: the images a workflow may run, mapped to the pre-registered
AWS Batch job definitions to run them with.

A deployment that registers one job definition per image ahead of time (so the
principal that runs Snakemake never needs ``batch:RegisterJobDefinition``) passes the
map with ``--aws-batch-job-definition-map`` (or
``SNAKEMAKE_AWS_BATCH_JOB_DEFINITION_MAP``). Every job's image is resolved as without
the map (including through the container image map, when there is one), then looked up
in this map, and the job is submitted against the definition it names, with the per-job
command, environment and resources in ``containerOverrides``. A job whose image is not
in the map fails without being submitted.

The map is a JSON object of image reference to job definition (a name, a
``name:revision`` pair or a job definition ARN with a revision), given inline or as the
path of a file. Inline JSON is for the command-line option and the environment
variable; in a Snakemake profile, give the path of a file. Snakemake drops an unquoted
mapping there, and on Python before 3.14 a quoted inline value can make it fail to load
the profile (``ENAMETOOLONG`` from its check of whether the value is a path). Images are
looked up as written, then in their normalized form, as in the container image map.
"""

import re
from typing import Any, Dict, Optional

from botocore.utils import ArnParser, InvalidArnException

from snakemake_executor_plugin_aws_batch.batch_client import BatchClient
from snakemake_executor_plugin_aws_batch.image_map import _ImageKeyedMap, _is_reference

# A job definition name (letters, digits, hyphens and underscores, up to 128
# characters), optionally with a revision (a positive integer: revisions start at 1).
_NAME_AND_REVISION = re.compile(
    r"(?P<name>[A-Za-z0-9_-]{1,128})(?::(?P<revision>[1-9][0-9]*))?"
)

# The forms a mapped job definition may take, for error messages.
_FORMS = "a name, a name:revision pair, or a job definition ARN with a revision"


def job_definition_name(job_definition: str) -> Optional[str]:
    """The name of ``job_definition`` when it has no revision, ``None`` when it has
    one. Raises ``ValueError`` when it is not a ``name``, a ``name:revision`` pair or
    a job definition ARN with a revision.

    A revision-less ARN is refused: ``DescribeJobDefinitions`` can only find it by
    name, in the caller's own account and region, which need not be the ARN's.
    """
    if not job_definition.startswith("arn:"):
        match = _NAME_AND_REVISION.fullmatch(job_definition)
        if match is None:
            raise ValueError(f"{job_definition!r} is not {_FORMS}")
        return None if match["revision"] else match["name"]
    # arn:<partition>:batch:<region>:<account>:job-definition/<name>:<revision>
    not_an_arn = ValueError(f"{job_definition!r} is not a job definition ARN")
    try:
        arn = ArnParser().parse_arn(job_definition)
    except InvalidArnException:
        raise not_an_arn from None
    prefix = "job-definition/"
    if (
        arn["service"] != "batch"
        or not arn["partition"]
        or not arn["region"]
        or re.fullmatch(r"[0-9]{12}", arn["account"]) is None
        or not arn["resource"].startswith(prefix)
    ):
        raise not_an_arn
    match = _NAME_AND_REVISION.fullmatch(arn["resource"][len(prefix) :])
    if match is None:
        raise not_an_arn
    if not match["revision"]:
        raise ValueError(
            f"{job_definition!r} has no revision: a job definition ARN must end in "
            "':<revision>' (or give the bare name to use its latest revision)"
        )
    return None


def arn_region(job_definition: str) -> Optional[str]:
    """The region of ``job_definition`` when it is an ARN, else ``None``."""
    if not job_definition.startswith("arn:"):
        return None
    return ArnParser().parse_arn(job_definition)["region"]


class JobDefinitionMap(_ImageKeyedMap):
    """Image references to the pre-registered job definitions to run them with."""

    KIND = "job definition map"
    VALUES = "job definitions"
    NOT_AN_OBJECT = "a job definition map must be a JSON object"
    MISSING = (
        "{job_name}: container image {image!r} has no entry in the job definition "
        "map{where}; register a job definition for it and add it to the map"
    )

    def _check_entry(self, ref: Any, value: Any) -> None:
        """Refuse a malformed entry at startup, not at the first ``SubmitJob``."""
        where = f"job definition map entry {ref!r}"
        if not _is_reference(ref):
            raise ValueError(f"{where}: the key is not an image reference")
        if not isinstance(value, str):
            raise ValueError(f"{where}: {value!r} is not {_FORMS}")
        try:
            job_definition_name(value)
        except ValueError as e:
            raise ValueError(f"{where}: {e}") from e

    @staticmethod
    def is_inline(value: str) -> bool:
        """Whether a setting's value is the map itself (its first non-blank
        character is ``{``) rather than the path of a file holding it."""
        return value.lstrip().startswith("{")

    @classmethod
    def from_setting(cls, value: str) -> "JobDefinitionMap":
        """Load the map from a setting's value: a JSON object, or a file's path
        (see :meth:`is_inline`). Only this map takes inline JSON; the container
        image map takes a file path only."""
        if cls.is_inline(value):
            return cls.from_json(value)
        return cls.from_file(value)


def _describe_request(job_definition: str) -> Dict[str, Any]:
    """The ``DescribeJobDefinitions`` arguments that find ``job_definition``: a
    revision (``name:revision`` or an ARN) through ``jobDefinitions``, a bare name
    through ``jobDefinitionName`` (its ACTIVE revisions, of which ``SubmitJob`` uses
    the highest)."""
    name = job_definition_name(job_definition)
    if name is None:
        return {"jobDefinitions": [job_definition]}
    return {"jobDefinitionName": name, "status": "ACTIVE"}


def describe_job_definition(
    batch_client: BatchClient, job_definition: str
) -> Optional[dict]:
    """Describe the revision of ``job_definition`` that ``SubmitJob`` would use, or
    return ``None`` when there is none.

    ``job_definition`` is in a form the map accepts: ``name``, ``name:revision``, or
    a job definition ARN with a revision. A bare name is its highest ACTIVE revision
    at the time of the call. Errors from the API are not caught.
    """
    definitions = batch_client.describe_job_definitions(
        **_describe_request(job_definition)
    )
    if not definitions:
        return None
    return max(definitions, key=lambda d: d.get("revision", 0))
