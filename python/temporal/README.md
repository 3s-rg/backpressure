# backpressure-temporal

Temporal plugin that gates activities with queued Valkey admission (`backpressure.queued_acquire`) via local activities, keeping the Temporal matching service out of the admission path.

## How it works

`BackpressurePlugin` is a `SimplePlugin` that registers two internal Temporal **local activities** (`__bp_acquire` / `__bp_release`) on every worker that loads the plugin.  Local activities execute in-process without going through the Temporal matching service, the Valkey poll loop never holds a regular activity slot.

When `gated_activity(...)` is called from a workflow:

1. `__bp_acquire` runs as a local activity: polls Valkey via `queued_acquire(..., blocking=True)`, emits `backpressure_queued_acquire_wait_ms` / `backpressure_acquire_eval_ms` / `backpressure_acquire_polls` metrics, and returns `AcquireStats` or raises a typed `ApplicationError` on denial / timeout.
2. The regular user activity is dispatched via matching, the slot is held only for the duration of the actual work.
3. If the user activity raises, `__bp_release` runs as a local activity to refund the token.

`gated_activity` is the single entry point.  The `activity_targets` map in `BackpressurePlugin` is used to resolve `target_id` automatically from the activity name.

Direct Python callers use `queued_acquire` / `queued_release` from the [`backpressure`](../core) package.

## Setup

```python
from backpressure_temporal import BackpressurePlugin

plugin = BackpressurePlugin.from_config("backpressure.yaml", valkey_url="redis://localhost:6379")

worker = Worker(
    client,
    task_queue="my-queue",
    activities=[call_llm],
    workflows=[MyWorkflow],
    plugins=[plugin],
)
```

Or construct directly:

```python
from backpressure.types import Constraint, TargetConfig

plugin = BackpressurePlugin(
    valkey_url="redis://localhost:6379",
    targets={
        "openai": TargetConfig(
            target_id="openai",
            constraints=[
                Constraint(dimension="tokens", limit=100_000, default_cost=500),
            ],
            tenant_weights={"team_a": 2.0, "team_b": 1.0},
            priority_minimums={1: 0.5, 2: 0.3, 3: 0.2},
        ),
    },
    activity_targets={"call_llm": "openai"},
)
```

## Config file format

JSON or YAML. YAML requires `pyyaml` (`pip install backpressure-temporal[yaml]`).

```yaml
targets:
  openai:
    constraints:
      - dimension: tokens
        limit: 100000
        default_cost: 500
    tenant_weights:
      team_a: 2.0
      team_b: 1.0
    priority_minimums:
      1: 0.5
      2: 0.3
      3: 0.2

activity_targets:
  call_llm: openai
  embed_text: openai
```

## Workflow usage

Every gated activity needs a **unique `request_id`** per admission attempt (deterministic and replay-safe):

```python
from backpressure_temporal import gated_activity

@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self, tenant_id: str) -> str:
        wid = workflow.info().workflow_id
        return await gated_activity(
            call_llm,
            "hello",
            tenant_id=tenant_id,
            priority=1,
            request_id=f"{wid}:0:1",
            start_to_close_timeout=timedelta(seconds=30),
        )
```

With a `default_cost` on every constraint, `resource_costs` can be omitted:

```python
result = await gated_activity(
    call_llm,
    prompt,
    tenant_id=tenant_id,
    priority=1,
    request_id=f"{wid}:1:1",
    resource_costs={"tokens": 500},
    start_to_close_timeout=timedelta(seconds=30),
)
```

## `GatedResult` vs plain return

`GatedResult` may be used to carry structured payloads. `gated_activity` unwraps `value` before returning to the workflow and merges queue timing fields (`queued_wait_ms`, `queued_polls`, `queue_enqueued_ms`, `queue_granted_ms`) into dict-shaped payloads.

## Timeout and retries

The acquire local activity uses `blocking=True` so `QueuedAcquireDeniedError` is never raised directly. `QueuedAcquireTimeoutError` maps to `ApplicationError` `backpressure:queued_timeout`, `gated_activity()` catches this (and `backpressure:denied`), sleeps briefly at workflow level, and retries with a fresh `request_id` suffix (`:g{n}`).

`gated_activity()` accepts `max_gate_retries` (default 10) to cap combined denial and timeout retries:

```python
result = await gated_activity(
    call_llm,
    "hello",
    tenant_id=tenant_id,
    priority=1,
    request_id=f"{wid}:retry-loop",
    max_gate_retries=20,
    start_to_close_timeout=timedelta(seconds=30),
)
```

Set `max_gate_retries=None` for unbounded retries (outer bound is the workflow's `run_timeout`).
