"""Unit tests for executor-settings validation.

Covers validate_job_role_setting, which makes job_role conditionally required so
the pre-existing-job-definition path stays reachable from the CLI.
"""

import pytest
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_aws_batch import validate_job_role_setting


def test_job_role_required_on_dynamic_path():
    # No pre-existing definition and no job_role: the dynamic path needs the role
    # to register a definition, so this must fail fast with a clear message.
    with pytest.raises(WorkflowError, match="aws-batch-job-role"):
        validate_job_role_setting(job_definition=None, job_role=None)


def test_job_role_optional_with_preexisting_definition():
    # A pre-existing definition bakes the role in externally, so omitting job_role
    # is the supported configuration and must not raise.
    validate_job_role_setting(job_definition="my-job-def", job_role=None)


def test_job_role_present_on_dynamic_path_is_accepted():
    # The dynamic path with a job_role supplied is the ordinary case.
    validate_job_role_setting(
        job_definition=None, job_role="arn:aws:iam::123456789:role/test-role"
    )


def test_job_role_with_preexisting_definition_not_rejected_here():
    # Rejecting job_role + a pre-existing definition is BatchJobBuilder's
    # responsibility (_validate_preexisting_compatibility); this settings-level
    # check only enforces presence on the dynamic path and must stay silent here.
    validate_job_role_setting(
        job_definition="my-job-def",
        job_role="arn:aws:iam::123456789:role/test-role",
    )
