"""Unit tests for queued_release refund path."""

from __future__ import annotations

import pytest
from fakeredis import FakeAsyncValkey, FakeServer

from backpressure.queued_gate import QueuedAcquireRequest, queued_acquire, queued_release
from backpressure.types import Constraint, TargetConfig


@pytest.fixture
async def valkey() -> FakeAsyncValkey:
    """In-memory Valkey for admission state."""
    server = FakeServer(server_type="valkey")
    async with FakeAsyncValkey(server=server) as r:
        yield r


@pytest.mark.asyncio
async def test_queued_release_refunds_after_acquire(valkey: FakeAsyncValkey) -> None:
    """Release increases budgets after a successful acquire debit."""
    target = TargetConfig(
        target_id="qx",
        constraints=[Constraint(dimension="rps", limit=200)],
        tenant_weights={"t1": 1.0},
        priority_minimums={1: 1.0, 2: 0.0, 3: 0.0},
    )
    await queued_acquire(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="t1",
            priority=1,
            resource_costs={"rps": 5.0},
            request_id="r1",
            timeout_s=30.0,
            blocking=True,
        ),
    )
    # Snapshot budget after debit
    tid = target.target_id
    g_before = float(await valkey.hget(f"bpq:{tid}:tb:global", "budget") or 0)

    await queued_release(
        valkey=valkey,
        target=target,
        tenant_id="t1",
        resource_costs={"rps": 5.0},
        priority=1,
    )

    g_after = float(await valkey.hget(f"bpq:{tid}:tb:global", "budget") or 0)
    assert g_after > g_before
