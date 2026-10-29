"""Start demo workflows for two tenants against the example worker.

Run `examples/temporal_worker.py` first, then:

    uv run python examples/temporal_starter.py
"""

import asyncio
import uuid
from typing import Any

from temporalio.client import Client, WorkflowHandle

TASK_QUEUE = "backpressure-example"


async def main() -> None:
    client = await Client.connect("localhost:7233")
    handles: list[WorkflowHandle[Any, str]] = []
    for tenant in ("team_a", "team_b"):
        for i in range(3):
            handle = await client.start_workflow(
                "DemoWorkflow",
                tenant,
                id=f"bp-demo-{tenant}-{i}-{uuid.uuid4().hex[:8]}",
                task_queue=TASK_QUEUE,
                result_type=str,
            )
            handles.append(handle)
    for handle in handles:
        print(handle.id, "->", await handle.result())


if __name__ == "__main__":
    asyncio.run(main())
