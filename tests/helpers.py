"""Helpers shared by the unit-test modules."""

from types import SimpleNamespace
from typing import Iterable
from unittest.mock import MagicMock, patch

from snakemake_interface_executor_plugins.settings import DeploymentMethod

from snakemake_executor_plugin_aws_batch import Executor, ExecutorSettings


def post_init_executor(
    deployment_methods: Iterable[DeploymentMethod] = (), **settings
) -> Executor:
    """Run Executor.__post_init__ with the given executor settings and software
    deployment methods (AWS and preflight mocked)."""
    executor = object.__new__(Executor)
    executor.logger = MagicMock()
    executor.workflow = SimpleNamespace(
        remote_execution_settings=SimpleNamespace(container_image="global:1"),
        executor_settings=ExecutorSettings(region="us-east-1", **settings),
        deployment_settings=SimpleNamespace(deployment_method=set(deployment_methods)),
    )
    with patch("snakemake_executor_plugin_aws_batch.BatchClient"), patch.object(
        Executor, "_preflight_validate"
    ):
        executor.__post_init__()
    return executor
