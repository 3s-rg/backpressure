"""Token-bucket admission: grant, deny, priority reserves, tenant fairness, release."""

from __future__ import annotations

import asyncio

import pytest
from fakeredis import FakeAsyncValkey, FakeServer

from backpressure.queued_gate import (
    QueuedAcquireDeniedError,
    QueuedAcquireRequest,
    queued_acquire,
    queued_release,
)
from backpressure.types import Constraint, TargetConfig


@pytest.fixture
async def valkey() -> FakeAsyncValkey:
    server = FakeServer(server_type="valkey")
    async with FakeAsyncValkey(server=server) as r:
        yield r


def _target(**kwargs: object) -> TargetConfig:
    base: dict[str, object] = {
        "target_id": "t",
        "constraints": [Constraint(dimension="rps", limit=100)],
    }
    base.update(kwargs)
    return TargetConfig(**base)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_under_cap_grants(valkey: FakeAsyncValkey) -> None:
    target = _target(tenant_weights={"a": 1.0}, priority_minimums={1: 1.0, 2: 0.0, 3: 0.0})
    stats = await queued_acquire(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rps": 1.0},
            request_id="x",
            timeout_s=5.0,
        ),
    )
    assert stats.polls >= 1
    assert stats.granted_at_ms > 0


@pytest.mark.asyncio
async def test_over_cap_denies_nonblocking(valkey: FakeAsyncValkey, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QUEUED_GATE_BURST_SECONDS", "0.01")
    target = _target(tenant_weights={"a": 1.0}, priority_minimums={1: 1.0, 2: 0.0, 3: 0.0})
    await queued_acquire(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rps": 1.0},
            request_id="x1",
            timeout_s=5.0,
        ),
    )
    with pytest.raises(QueuedAcquireDeniedError) as ei:
        await queued_acquire(
            valkey=valkey,
            target=target,
            request=QueuedAcquireRequest(
                tenant_id="a",
                priority=1,
                resource_costs={"rps": 100.0},
                request_id="x2",
                timeout_s=5.0,
                blocking=False,
            ),
        )
    assert ei.value.retry_after_ms >= 1


@pytest.mark.asyncio
async def test_refill_allows_subsequent_grant(valkey: FakeAsyncValkey, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QUEUED_GATE_BURST_SECONDS", "0.05")
    target = _target(tenant_weights={"a": 1.0}, priority_minimums={1: 1.0, 2: 0.0, 3: 0.0})
    await queued_acquire(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rps": 4.0},
            request_id="x1",
            timeout_s=5.0,
            blocking=True,
        ),
    )

    await asyncio.sleep(0.06)
    await queued_acquire(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rps": 2.0},
            request_id="x2",
            timeout_s=5.0,
            blocking=False,
        ),
    )


@pytest.mark.asyncio
async def test_oversized_request_denied_when_global_empty(valkey: FakeAsyncValkey) -> None:
    """Admission is gated only by the global cap.

    A request larger than current global capacity denies regardless of priority.
    """
    target = _target(
        tenant_weights={"a": 1.0, "b": 1.0},
        priority_minimums={1: 0.6, 2: 0.2, 3: 0.2},
    )
    for i in range(50):
        await queued_acquire(
            valkey=valkey,
            target=target,
            request=QueuedAcquireRequest(
                tenant_id="a",
                priority=1,
                resource_costs={"rps": 1.0},
                request_id=f"p1-{i}",
                timeout_s=30.0,
            ),
        )
    with pytest.raises(QueuedAcquireDeniedError):
        await queued_acquire(
            valkey=valkey,
            target=target,
            request=QueuedAcquireRequest(
                tenant_id="b",
                priority=3,
                resource_costs={"rps": 500.0},
                request_id="p3-big",
                timeout_s=1.0,
                blocking=False,
            ),
        )


@pytest.mark.asyncio
async def test_tenant_fairness_heavy_and_light(valkey: FakeAsyncValkey) -> None:
    """Both tenants P1 with same priority get grants under moderate load (weight table denominator)."""
    target = _target(tenant_weights={"heavy": 3.0, "light": 1.0})
    for i in range(5):
        await queued_acquire(
            valkey=valkey,
            target=target,
            request=QueuedAcquireRequest(
                tenant_id="heavy",
                priority=1,
                resource_costs={"rps": 1.0},
                request_id=f"h-{i}",
                timeout_s=10.0,
            ),
        )
        await queued_acquire(
            valkey=valkey,
            target=target,
            request=QueuedAcquireRequest(
                tenant_id="light",
                priority=1,
                resource_costs={"rps": 1.0},
                request_id=f"l-{i}",
                timeout_s=10.0,
            ),
        )


@pytest.mark.asyncio
async def test_tp_sweep_removes_stale_bucket_field(valkey: FakeAsyncValkey, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stale tb:tp rows (last_used older than idle_ms) are HDEL'd on acquire.

    They do not inflate the fair-share denominator forever.
    """
    monkeypatch.setenv("QUEUED_GATE_REGISTRY_IDLE_MS", "500")
    target = _target(tenant_weights={"a": 1.0})
    tid = target.target_id
    tp_key = f"bpq:{tid}:tb:tp"
    await valkey.hset(tp_key, "ghost:1", "0:1")
    await queued_acquire(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rps": 1.0},
            request_id="sweep-1",
            timeout_s=5.0,
        ),
    )
    assert await valkey.hget(tp_key, "ghost:1") is None


@pytest.mark.asyncio
async def test_release_restores_capacity(valkey: FakeAsyncValkey) -> None:
    target = _target(tenant_weights={"a": 1.0})
    await queued_acquire(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=2,
            resource_costs={"rps": 10.0},
            request_id="acq",
            timeout_s=10.0,
        ),
    )
    g_mid = float(await valkey.hget(f"bpq:{target.target_id}:tb:global:rps", "budget") or 0)
    await queued_release(valkey=valkey, target=target, tenant_id="a", resource_costs={"rps": 10.0}, priority=2)
    g_after = float(await valkey.hget(f"bpq:{target.target_id}:tb:global:rps", "budget") or 0)
    assert g_after >= g_mid
