"""Tests for GatedResult, error-type constants, and DEFAULT_MAX_GATE_RETRIES."""

from __future__ import annotations

from backpressure_temporal.gate import (
    DEFAULT_MAX_GATE_RETRIES,
    DENIED_ERROR_TYPE,
    QUEUED_TIMEOUT_TYPE,
    GatedResult,
)


def test_gated_result_fields() -> None:
    """GatedResult stores value and actual_costs."""
    r: GatedResult[str] = GatedResult(value="hello", actual_costs={"tokens": 347})
    assert r.value == "hello"
    assert r.actual_costs == {"tokens": 347}


def test_gated_result_isinstance_check() -> None:
    """GatedResult is detected by isinstance."""
    r: GatedResult[str] = GatedResult(value="ok", actual_costs={"rps": 1})
    assert isinstance(r, GatedResult)


def test_default_max_gate_retries_value() -> None:
    """DEFAULT_MAX_GATE_RETRIES is 10."""
    assert DEFAULT_MAX_GATE_RETRIES == 10


def test_denied_error_type_constant() -> None:
    assert DENIED_ERROR_TYPE == "backpressure:denied"


def test_queued_timeout_error_type_constant() -> None:
    assert QUEUED_TIMEOUT_TYPE == "backpressure:queued_timeout"
