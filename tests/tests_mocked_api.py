from unittest.mock import AsyncMock, MagicMock, patch  # noqa
from tests import TestWorkflowsBase


class TestWorkflowsMocked(TestWorkflowsBase):
    __test__ = True

    def run_workflow(self, *args, **kwargs):
        super().run_workflow(*args, **kwargs)
