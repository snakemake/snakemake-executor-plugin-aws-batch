"""A rule queue map: rule names mapped to the AWS Batch job queues their jobs run on.

A deployment that routes a workflow's rules to different queues (for example GPU rules
to a GPU queue) without editing the workflow passes the map with
``--aws-batch-rule-queues`` (or ``SNAKEMAKE_AWS_BATCH_RULE_QUEUES``). Unlike
``--set-resources``, which replaces a workflow profile's whole ``set-resources`` when
given on the command line, the map leaves every other resource alone.

The map is a JSON object of rule name to job queue (a name or a job queue ARN), given
inline or as the path of a file holding it. A job's queue is the map's entry for its
rule, else the rule's ``batch_queue`` resource, else ``--aws-batch-job-queue``. A group
job runs as one AWS Batch job, so its rules must all map to the same queue, or none of
them be in the map.
"""

import re
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.jobs import (
    GroupJobExecutorInterface,
    JobExecutorInterface,
)

from snakemake_executor_plugin_aws_batch.json_map import load_json_object

# A job queue name, as CreateJobQueue accepts it.
_QUEUE_NAME = r"[A-Za-z0-9_-]{1,128}"

# A job queue as SubmitJob's jobQueue takes it: its name, or its ARN.
_QUEUE_ARN = r"arn:aws(?:-[a-z]+)*:batch:[a-z0-9-]+:[0-9]{12}:job-queue/" + _QUEUE_NAME
_JOB_QUEUE = re.compile(rf"{_QUEUE_NAME}|{_QUEUE_ARN}")

# What a job queue looks like, for error messages.
_JOB_QUEUE_FORM = (
    "a name of up to 128 letters, digits, - and _, or "
    "arn:aws:batch:<region>:<12-digit account>:job-queue/<name>"
)


class RuleQueues:
    """Rule names to the job queues their jobs run on."""

    def __init__(self, mapping: Mapping[str, Any]):
        self._entries: Dict[str, str] = {}
        for rule, queue in mapping.items():
            # Any non-empty string: Snakemake names a rule with any string its name:
            # directive gives, and an unnamed rule by its number ("1", "2", ...).
            if not isinstance(rule, str) or not rule:
                raise ValueError(f"rule queue map key {rule!r} is not a rule name")
            if not isinstance(queue, str) or not _JOB_QUEUE.fullmatch(queue):
                raise ValueError(
                    f"rule queue map entry {rule!r}: {queue!r} is not a job queue name "
                    f"or job queue ARN (expected {_JOB_QUEUE_FORM})"
                )
            self._entries[rule] = queue

    @classmethod
    def from_json(cls, text: str) -> "RuleQueues":
        """Load a JSON object of rule name -> job queue."""
        return cls(load_json_object(text, "rule", "a rule queue map"))

    @staticmethod
    def is_inline(value: str) -> bool:
        """Whether a setting's value is the JSON object itself (its first non-blank
        character is ``{``) rather than the path of a file holding it."""
        return value.lstrip().startswith("{")

    @classmethod
    def from_setting(cls, value: str) -> "RuleQueues":
        """Load the map from a setting's value: the JSON object itself, or the path
        of a file holding it (see ``is_inline``)."""
        if cls.is_inline(value):
            return cls.from_json(value)
        try:
            text = Path(value).expanduser().read_text(encoding="utf-8")
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"{e}; the value is read as a file path because it does not start "
                "with '{' (an inline map must be a JSON object)"
            ) from e
        return cls.from_json(text)

    def queue_of(self, job: JobExecutorInterface) -> Optional[str]:
        """The queue the map gives ``job``, or ``None`` when its rule is not in the map.

        A group job takes the queue its rules map to. Raises ``WorkflowError`` when its
        rules map to different queues, or only some of them are in the map: the group
        runs as one AWS Batch job, on one queue.
        """
        if not isinstance(job, GroupJobExecutorInterface):
            return self._entries.get(job.name)
        rules = sorted({member.name for member in job.jobs})
        queues = {rule: self._entries.get(rule) for rule in rules}
        if len(set(queues.values())) <= 1:
            return next(iter(queues.values()), None)
        listed = ", ".join(
            f"{rule} -> {queue if queue is not None else '(not in the map)'}"
            for rule, queue in queues.items()
        )
        raise WorkflowError(
            f"{job.name}: the rules of this group job do not share one queue in the "
            f"rule queue map ({listed}). A group runs as one AWS Batch job, so map all "
            "of its rules to the same queue, or leave all of them out of the map"
        )

    def __len__(self) -> int:
        """The number of rules in the map."""
        return len(self._entries)
