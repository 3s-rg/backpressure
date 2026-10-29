"""Shared data types for the backpressure gate module."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

__all__ = ["Constraint", "TargetConfig"]


@dataclass(kw_only=True)
class Constraint:
    """A single rate-limit rule for one resource dimension.

    Each constraint defines one resource dimension (e.g. RPM, TPM).
    ``queued_acquire`` enforces **all** entries in ``TargetConfig.constraints``
    atomically; per-request cost comes from ``resource_costs[dimension]`` or
    ``default_cost``.
    """

    dimension: str
    """Resource dimension name (e.g. ``"rpm"``, ``"tokens"``)."""

    limit: float
    """Target rate in requests (or units) per second used as the global refill rate."""

    default_cost: float | None = None
    """Cost charged per request when ``resource_costs`` omits this dimension."""

    def __post_init__(self) -> None:
        """Validate constraint fields after construction."""
        if self.limit <= 0:
            msg = f"limit must be positive, got {self.limit}"
            raise ValueError(msg)


@dataclass
class TargetConfig:
    """Full configuration for a rate-limited upstream target.

    A target identifies a single upstream service or API surface (e.g. ``"openai-gpt4"``).
    All tenants competing for the same upstream share the same ``TargetConfig``.
    """

    target_id: str
    """Unique identifier; used as the Valkey key prefix ``bpq:{target_id}:tb:*``."""

    constraints: list[Constraint]
    """At least one constraint.  All dimensions are enforced in one atomic admission."""

    tenant_weights: dict[str, float] = field(default_factory=dict)
    """Per-tenant weight for weighted-fair-share allocation **within** each priority class.
    Tenants absent from the map receive ``default_weight`` (the minimum configured weight,
    or 1.0 when the map is empty).  Only tenants with recent bucket state in Valkey
    (non-stale) participate in the active-weight denominator.
    """

    priority_minimums: dict[int, float] = field(default_factory=dict)
    """Per-priority refill-share fractions for ``queued_acquire`` (keys 1, 2, 3).

    Each value is the fraction of ``limit_rps`` refilled into that priority's bookkeeping
    bucket per second; values are **normalized** to sum 1.0 before use.

    Defaults to ``{1: 0.5, 2: 0.3, 3: 0.2}`` when empty.
    P1 = highest priority (shortest retry backoff, 50 % of refill budget by default).
    """

    @property
    def default_weight(self) -> float:
        """Minimum configured tenant weight, or 1.0 when no weights are set."""
        if self.tenant_weights:
            return min(self.tenant_weights.values())
        return 1.0

    def to_dict(self) -> dict[str, object]:
        """Produce a JSON-serializable dictionary representation."""
        return {
            "target_id": self.target_id,
            "constraints": [
                {
                    "dimension": c.dimension,
                    "limit": c.limit,
                    **({"default_cost": c.default_cost} if c.default_cost is not None else {}),
                }
                for c in self.constraints
            ],
            "tenant_weights": dict(self.tenant_weights),
            "priority_minimums": {str(k): v for k, v in self.priority_minimums.items()},
        }

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> TargetConfig:
        """Reconstruct a ``TargetConfig`` from a dictionary produced by ``to_dict``."""
        raw_constraints = cast("list[dict[str, object]]", d.get("constraints", []))
        constraints = [
            Constraint(
                dimension=str(c["dimension"]),
                limit=cast("float", c["limit"]),
                default_cost=cast("float | None", c.get("default_cost")),
            )
            for c in raw_constraints
        ]
        raw_priority = cast("dict[str, float]", d.get("priority_minimums", {}))
        priority_minimums = {int(k): v for k, v in raw_priority.items()}
        return cls(
            target_id=str(d["target_id"]),
            constraints=constraints,
            tenant_weights=cast("dict[str, float]", d.get("tenant_weights", {})),
            priority_minimums=priority_minimums,
        )
