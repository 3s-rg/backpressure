"""Degraded-open smoke test (invariant I3) against a real Valkey in Docker.

Phase 1: Valkey up (docker compose), gated calls are admitted normally.
Phase 2: Valkey container stopped, calls must still succeed via fail-open.
Phase 3: Valkey restarted, the gate resumes normal admission.

Manual script, not collected by pytest. Run from ``temporal/`` with the
``valkey`` compose service up (``docker compose up -d valkey`` at repo root):

    uv run python tests/integration/smoke_fail_open.py

Success = phase-2 workflows complete while ``backpressure degraded open``
warnings appear, and phase 3 completes without warnings.
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from valkey.asyncio import Valkey

from backpressure.types import Constraint, TargetConfig
from backpressure_temporal import _local_activities as _la
from backpressure_temporal import gated_activity
from backpressure_temporal._local_activities import bp_acquire, bp_release

TARGET_ID = "smoke-target"
TASK_QUEUE = "smoke-fail-open"
VALKEY_CONTAINER = "backpressure-valkey-1"


@activity.defn(name="probe_activity")
async def probe_activity(tag: str) -> dict[str, Any]:
    """Return a dict result so the gate can merge queue stats into it."""
    return {"status": "ok", "tag": tag}


@workflow.defn(sandboxed=False)
class SmokeWorkflow:
    """One gated ``probe_activity`` call per run."""

    @workflow.run
    async def run(self, tag: str) -> dict[str, Any]:
        """Issue a single gated probe request for *tag*."""
        wid = workflow.info().workflow_id
        result: dict[str, Any] = await gated_activity(
            probe_activity,
            tag,
            tenant_id="t1",
            priority=1,
            request_id=f"{wid}:{tag}",
            resource_costs={"rps": 1.0},
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return result


def _docker(cmd: str) -> None:
    """Run ``docker <cmd>`` against the Valkey container (trusted, fixed argv)."""
    subprocess.run(  # noqa: S603
        ["/usr/local/bin/docker", cmd, VALKEY_CONTAINER],
        check=True,
        capture_output=True,
    )


async def _run_phase(env: WorkflowEnvironment, label: str, count: int = 3) -> None:
    """Execute *count* smoke workflows and report their outcome."""
    for i in range(count):
        result = await env.client.execute_workflow(
            SmokeWorkflow.run,
            f"{label}-{i}",
            id=f"smoke-{label}-{i}",
            task_queue=TASK_QUEUE,
        )
        print(f"  {label}-{i}: status={result.get('status')}")  # noqa: T201


async def main() -> None:
    """Drive the three-phase kill/restore smoke sequence."""
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    _la.valkey_pool = Valkey.from_url("redis://localhost:6379")
    _la.targets = {
        TARGET_ID: TargetConfig(
            target_id=TARGET_ID,
            constraints=[Constraint(dimension="rps", limit=200.0)],
            tenant_weights={"t1": 1.0},
        ),
    }
    _la.activity_target_map = {"probe_activity": TARGET_ID}

    env = await WorkflowEnvironment.start_local()
    try:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[SmokeWorkflow],
            activities=[probe_activity, bp_acquire, bp_release],
        ):
            print("== phase 1: valkey up, expect normal admission")  # noqa: T201
            await _run_phase(env, "up")
            print("== phase 2: stopping valkey, expect degraded-open grants")  # noqa: T201
            _docker("stop")
            await _run_phase(env, "down")
            print("== phase 3: restarting valkey, expect normal admission again")  # noqa: T201
            _docker("start")
            await asyncio.sleep(2.0)
            await _run_phase(env, "back")
    finally:
        await env.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
