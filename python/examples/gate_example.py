"""Standalone admission-gate demo (no Temporal).

Two tenants share a 5 req/s target. team_a has twice team_b's weight, so
under sustained contention it is granted a larger share of the throughput.

Requires a Valkey on localhost:6379 (from the repo root: `docker compose up -d valkey`).
Run from `python/`:

    uv run python examples/gate_example.py
"""

import asyncio
import sys
import time

from valkey.asyncio import Valkey

from backpressure import Constraint, QueuedAcquireRequest, TargetConfig, queued_acquire

LIMIT_RPS = 5.0
RUN_SECONDS = 10.0
CALLERS_PER_TENANT = 4
# The bucket starts full and refills at LIMIT_RPS, so a run may exceed the
# steady-state rate by roughly one burst window. Anything beyond that head-
# room means the cap was not enforced.
MAX_ALLOWED_RATE = LIMIT_RPS * 1.5

# A run-scoped target id so every run starts with fresh bucket state instead
# of inheriting whatever an earlier demo left behind.
TARGET = TargetConfig(
    target_id=f"demo-api-{int(time.time())}",
    constraints=[Constraint(dimension="rps", limit=LIMIT_RPS)],
    tenant_weights={"team_a": 2.0, "team_b": 1.0},
)


class TenantStats:
    def __init__(self) -> None:
        self.grants = 0
        self.wait_ms_total = 0

    def mean_wait_ms(self) -> float:
        return self.wait_ms_total / self.grants if self.grants else 0.0


async def tenant_loop(
    valkey: Valkey,
    tenant_id: str,
    stats_by_tenant: dict[str, TenantStats],
    started: float,
    deadline: float,
) -> None:
    n = 0
    while time.monotonic() < deadline:
        n += 1
        stats = await queued_acquire(
            valkey=valkey,
            target=TARGET,
            request=QueuedAcquireRequest(
                tenant_id=tenant_id,
                priority=1,
                resource_costs={"rps": 1.0},
                request_id=f"{tenant_id}-{n}",
                timeout_s=30.0,
            ),
        )
        # Permit granted and already debited: call the rate-limited upstream
        # here. On upstream failure, refund with queued_release().
        t = stats_by_tenant[tenant_id]
        t.grants += 1
        t.wait_ms_total += stats.wait_ms
        elapsed = time.monotonic() - started
        print(f"[{elapsed:5.1f}s] {tenant_id} granted #{t.grants} (waited {stats.wait_ms} ms, {stats.polls} polls)")


async def main() -> None:
    valkey = Valkey.from_url("redis://localhost:6379", decode_responses=False)
    stats_by_tenant = {"team_a": TenantStats(), "team_b": TenantStats()}

    print(
        f"target {TARGET.target_id}: {LIMIT_RPS:.0f} req/s, weights team_a=2.0 team_b=1.0, "
        f"{CALLERS_PER_TENANT} concurrent callers per tenant, {RUN_SECONDS:.0f} s run"
    )
    started = time.monotonic()
    deadline = started + RUN_SECONDS
    async with asyncio.TaskGroup() as tg:
        for tenant in stats_by_tenant:
            for _ in range(CALLERS_PER_TENANT):
                tg.create_task(tenant_loop(valkey, tenant, stats_by_tenant, started, deadline))
    await valkey.aclose()

    total = sum(t.grants for t in stats_by_tenant.values())
    rate = total / RUN_SECONDS
    print()
    print("summary")
    print("-------")
    for tenant, t in stats_by_tenant.items():
        print(f"  {tenant}: {t.grants} grants, mean wait {t.mean_wait_ms():.0f} ms")
    print(f"  total : {total} grants in {RUN_SECONDS:.0f} s (~{rate:.1f}/s against a {LIMIT_RPS:.0f}/s cap)")

    cap_ok = rate <= MAX_ALLOWED_RATE
    all_served = all(t.grants > 0 for t in stats_by_tenant.values())
    if cap_ok and all_served:
        print(f"SUCCESS: cap enforced ({rate:.1f}/s <= {MAX_ALLOWED_RATE:.1f}/s) and every tenant was served")
    else:
        print(f"FAILURE: cap_ok={cap_ok} all_served={all_served}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
