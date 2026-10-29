"""Tests for gate shared data types."""

import json
from typing import cast

import pytest

from backpressure.types import Constraint, TargetConfig


def test_construct_dataclasses() -> None:
    """Construct each dataclass with valid inputs and verify field values."""
    c = Constraint(dimension="rpm", limit=600.0)
    assert c.dimension == "rpm"
    assert c.limit == 600.0
    assert c.default_cost is None

    tc = TargetConfig(
        target_id="gpt-4",
        constraints=[c],
        tenant_weights={"t1": 0.7, "t2": 0.3},
    )
    assert tc.target_id == "gpt-4"
    assert len(tc.constraints) == 1
    assert tc.tenant_weights == {"t1": 0.7, "t2": 0.3}
    assert tc.priority_minimums == {}


def test_default_weight() -> None:
    """default_weight returns min weight when present, 1.0 when empty."""
    config_with_weights = TargetConfig(
        target_id="t",
        constraints=[],
        tenant_weights={"a": 0.8, "b": 0.2, "c": 0.5},
    )
    assert config_with_weights.default_weight == 0.2

    config_empty = TargetConfig(target_id="t", constraints=[])
    assert config_empty.default_weight == 1.0


def test_to_dict_from_dict_roundtrip() -> None:
    """to_dict / from_dict roundtrip preserves equality."""
    config = TargetConfig(
        target_id="gpt-4",
        constraints=[
            Constraint(dimension="rpm", limit=600.0),
            Constraint(dimension="tpm", limit=40000.0),
            Constraint(dimension="rpd", limit=10000.0),
        ],
        tenant_weights={"team-a": 0.6, "team-b": 0.3, "team-c": 0.1},
        priority_minimums={1: 0.5, 2: 0.3, 3: 0.2},
    )
    assert TargetConfig.from_dict(config.to_dict()) == config


def test_to_dict_json_serializable() -> None:
    """to_dict output can be passed to json.dumps without error."""
    config = TargetConfig(
        target_id="gpt-4",
        constraints=[Constraint(dimension="rpm", limit=600.0)],
        tenant_weights={"team-a": 1.0},
    )
    serialized = json.dumps(config.to_dict())
    assert isinstance(serialized, str)


def test_constraint_zero_limit_raises() -> None:
    """Constraint with limit=0 raises ValueError."""
    with pytest.raises(ValueError, match="limit must be positive"):
        Constraint(dimension="rpm", limit=0)


def test_constraint_negative_limit_raises() -> None:
    """Constraint with negative limit raises ValueError."""
    with pytest.raises(ValueError, match="limit must be positive"):
        Constraint(dimension="rpm", limit=-5)


def test_constraint_default_cost_none_by_default() -> None:
    """default_cost is None when not specified."""
    c = Constraint(dimension="rpm", limit=10)
    assert c.default_cost is None


def test_constraint_default_cost_set() -> None:
    """default_cost can be set at construction."""
    c = Constraint(dimension="rpm", limit=10, default_cost=1.0)
    assert c.default_cost == 1.0


def test_to_dict_includes_default_cost_when_set() -> None:
    """to_dict includes default_cost only when it is not None."""
    c_with = Constraint(dimension="rpm", limit=10, default_cost=1.0)
    c_without = Constraint(dimension="tpm", limit=500)

    config = TargetConfig(target_id="t", constraints=[c_with, c_without])
    d = config.to_dict()
    constraints = cast("list[dict[str, object]]", d["constraints"])
    assert "default_cost" in constraints[0]
    assert constraints[0]["default_cost"] == 1.0
    assert "default_cost" not in constraints[1]


def test_to_dict_from_dict_roundtrip_with_default_cost() -> None:
    """to_dict / from_dict roundtrip preserves default_cost."""
    config = TargetConfig(
        target_id="t",
        constraints=[
            Constraint(dimension="rpm", limit=10, default_cost=1.0),
            Constraint(dimension="tpm", limit=500),
        ],
    )
    restored = TargetConfig.from_dict(config.to_dict())
    assert restored == config
    assert restored.constraints[0].default_cost == 1.0
    assert restored.constraints[1].default_cost is None
