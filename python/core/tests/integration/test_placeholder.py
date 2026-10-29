"""Placeholder so ``pytest -m integration`` selects at least one test in core."""

import pytest


@pytest.mark.integration
def test_core_integration_placeholder() -> None:
    """Live Valkey + mocklimit scenarios lived here before the direct gate path was removed."""
    pytest.skip("No core integration tests in queued-only build")
