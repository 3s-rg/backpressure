"""Temporal worker wired up with the Backpressure plugin.

Requires Valkey (`docker compose up -d valkey` from the repo root) and a
Temporal dev server (`temporal server start-dev`). Run from `python/`:

    uv run python examples/temporal_worker.py

Then, in another shell, start workflows:

    uv run python examples/temporal_starter.py
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

from backpressure import Constraint, TargetConfig
from backpressure_temporal import BackpressurePlugin, gated_activity

TASK_QUEUE = "backpressure-example"


@activity.defn(name="call_upstream")
async def call_upstream(payload: str) -> str:
    # Stand-in for the rate-limited upstream call.
    await asyncio.sleep(0.05)
    return f"ok:{payload}"


@workflow.defn(name="DemoWorkflow")
class DemoWorkflow:
    @workflow.run
    async def run(self, tenant_id: str) -> str:
        results: list[str] = []
        for i in range(3):
            result = await gated_activity(
                "call_upstream",
                f"{tenant_id}-{i}",
                tenant_id=tenant_id,
                priority=1,
                request_id=f"{workflow.info().workflow_id}:{i}",
                start_to_close_timeout=timedelta(seconds=30),
            )
            results.append(str(result))
        return ", ".join(results)


async def main() -> None:
    client = await Client.connect("localhost:7233")

    plugin = BackpressurePlugin(
        valkey_url="redis://localhost:6379",
        targets={
            "demo-api": TargetConfig(
                target_id="demo-api",
                constraints=[Constraint(dimension="rps", limit=5.0)],
                tenant_weights={"team_a": 2.0, "team_b": 1.0},
            ),
        },
        activity_targets={"call_upstream": "demo-api"},
    )

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[DemoWorkflow],
        activities=[call_upstream],
        plugins=[plugin],
    )
    print(f"worker polling {TASK_QUEUE!r}, Ctrl+C to stop")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
