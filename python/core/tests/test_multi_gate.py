"""Atomic multi-dimensional admission: grant, deny, DRF L3, release."""

from __future__ import annotations

import asyncio

import pytest
from fakeredis import FakeAsyncValkey, FakeServer

from backpressure.queued_gate import (
    QueuedAcquireDeniedError,
    QueuedAcquireRequest,
    queued_acquire_multi,
    queued_release_multi,
)
from backpressure.types import Constraint, TargetConfig


@pytest.fixture
async def valkey() -> FakeAsyncValkey:
    server = FakeServer(server_type="valkey")
    async with FakeAsyncValkey(server=server) as r:
        yield r


def _two_dim_target(**kwargs: object) -> TargetConfig:
    base: dict[str, object] = {
        "target_id": "tm",
        "constraints": [
            Constraint(dimension="rpm", limit=60.0, default_cost=1.0),
            Constraint(dimension="tpm", limit=12_000.0, default_cost=200.0),
        ],
    }
    base.update(kwargs)
    return TargetConfig(**base)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_under_cap_grants(valkey: FakeAsyncValkey) -> None:
    target = _two_dim_target(
        tenant_weights={"a": 1.0},
        priority_minimums={1: 1.0, 2: 0.0, 3: 0.0},
    )
    stats = await queued_acquire_multi(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rpm": 1.0, "tpm": 100.0},
            request_id="x",
            timeout_s=5.0,
        ),
    )
    assert stats.polls >= 1
    assert stats.granted_at_ms > 0


@pytest.mark.asyncio
async def test_tpm_dominant_denies_when_tokens_exhaust_share(
    valkey: FakeAsyncValkey,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Tight burst so first request exhausts the tenant's share quickly.
    monkeypatch.setenv("QUEUED_GATE_BURST_SECONDS", "0.05")
    target = _two_dim_target(
        tenant_weights={"a": 1.0, "b": 1.0},
        priority_minimums={1: 1.0, 2: 0.0, 3: 0.0},
    )
    # Two tenants present at P1, each has rate_share=0.5. share_cost for a
    # 6000-token request = max(1/60, 6000/12000) = 0.5.
    await queued_acquire_multi(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rpm": 1.0, "tpm": 6_000.0},
            request_id="x1",
            timeout_s=5.0,
        ),
    )
    with pytest.raises(QueuedAcquireDeniedError) as ei:
        await queued_acquire_multi(
            valkey=valkey,
            target=target,
            request=QueuedAcquireRequest(
                tenant_id="a",
                priority=1,
                resource_costs={"rpm": 1.0, "tpm": 6_000.0},
                request_id="x2",
                timeout_s=5.0,
                blocking=False,
            ),
        )
    assert ei.value.retry_after_ms >= 1


@pytest.mark.asyncio
async def test_release_refunds_all_dims(
    valkey: FakeAsyncValkey,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QUEUED_GATE_BURST_SECONDS", "0.05")
    target = _two_dim_target(
        tenant_weights={"a": 1.0},
        priority_minimums={1: 1.0, 2: 0.0, 3: 0.0},
    )
    await queued_acquire_multi(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rpm": 1.0, "tpm": 6_000.0},
            request_id="x1",
            timeout_s=5.0,
        ),
    )
    # After release, a second acquire of the same cost should grant again.
    await queued_release_multi(
        valkey=valkey,
        target=target,
        tenant_id="a",
        resource_costs={"rpm": 1.0, "tpm": 6_000.0},
        priority=1,
    )
    await queued_acquire_multi(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rpm": 1.0, "tpm": 6_000.0},
            request_id="x2",
            timeout_s=5.0,
        ),
    )


@pytest.mark.asyncio
async def test_refill_allows_subsequent_grant(
    valkey: FakeAsyncValkey,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QUEUED_GATE_BURST_SECONDS", "0.05")
    target = _two_dim_target(
        tenant_weights={"a": 1.0},
        priority_minimums={1: 1.0, 2: 0.0, 3: 0.0},
    )
    await queued_acquire_multi(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rpm": 1.0, "tpm": 6_000.0},
            request_id="x1",
            timeout_s=5.0,
        ),
    )
    # Blocking acquire eventually succeeds after refill.
    stats = await queued_acquire_multi(
        valkey=valkey,
        target=target,
        request=QueuedAcquireRequest(
            tenant_id="a",
            priority=1,
            resource_costs={"rpm": 1.0, "tpm": 6_000.0},
            request_id="x2",
            timeout_s=5.0,
            blocking=True,
        ),
    )
    assert stats.polls >= 1


@pytest.mark.asyncio
async def test_drf_two_tenants_asymmetric_share(valkey: FakeAsyncValkey) -> None:
    """One heavy tenant + one light tenant; verify DRF dominant share.

    Heavy tenant uses 800-tok requests (TPM-dominant share = 800/12000 = 0.067).
    Light tenant uses 80-tok requests (RPM-dominant share = 1/60 = 0.0167).
    Both have weight 1; with two active tenants at P1 the per-tenant share rate
    is 0.5 share-units/sec.  Over wall time, light tenant should net more grants
    than heavy (light's per-call share-cost is 4x smaller).
    """
    target = _two_dim_target(
        tenant_weights={"heavy": 1.0, "light": 1.0},
        priority_minimums={1: 1.0, 2: 0.0, 3: 0.0},
    )

    grants_heavy = 0
    grants_light = 0

    async def tenant_loop(tenant: str, tokens: float, deadline: float) -> None:
        nonlocal grants_heavy, grants_light
        idx = 0
        while asyncio.get_event_loop().time() < deadline:
            idx += 1
            try:
                await queued_acquire_multi(
                    valkey=valkey,
                    target=target,
                    request=QueuedAcquireRequest(
                        tenant_id=tenant,
                        priority=1,
                        resource_costs={"rpm": 1.0, "tpm": tokens},
                        request_id=f"{tenant}:{idx}",
                        timeout_s=0.001,
                        blocking=False,
                    ),
                )
                if tenant == "heavy":
                    grants_heavy += 1
                else:
                    grants_light += 1
            except QueuedAcquireDeniedError as e:
                await asyncio.sleep(min(0.1, max(0.005, e.retry_after_ms / 1000.0)))

    deadline = asyncio.get_event_loop().time() + 1.5
    await asyncio.gather(
        tenant_loop("heavy", 800.0, deadline),
        tenant_loop("heavy", 800.0, deadline),
        tenant_loop("light", 80.0, deadline),
        tenant_loop("light", 80.0, deadline),
    )
    # Heavy should be significantly throttled vs light under DRF.
    assert grants_heavy < grants_light, (
        f"DRF should give heavy fewer grants than light; got heavy={grants_heavy} light={grants_light}"
    )
    # Heavy should still get *some* grants (no starvation).
    assert grants_heavy > 0
