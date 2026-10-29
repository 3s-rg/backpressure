# Python examples

Full runnable setups for both packages. All commands run from `python/` with the workspace synced (`uv sync`).

## Prerequisites

```bash
# from the repo root: start Valkey (the gate's backing store)
docker compose up -d valkey
```

## 1. Admission gate only (`backpressure`)

Two tenants with weights 2:1 hammer a 5 req/s target from 4 concurrent callers each. After 10 seconds the grant counts skew toward the heavier tenant.

```bash
uv run python examples/gate_example.py
```

## 2. Temporal integration (`backpressure-temporal`)

Additionally requires a local Temporal dev server:

```bash
temporal server start-dev
```

Terminal A, the worker (plugin + gated activity + workflow):

```bash
uv run python examples/temporal_worker.py
```

Terminal B, start six workflows across two tenants and print their results:

```bash
uv run python examples/temporal_starter.py
```

Set `BACKPRESSURE_GATE_MODE=inline` on the worker to fold admission and the activity body into a single dispatch (requires `inline_activities` in the plugin config, see the [`temporal/` README](../temporal/README.md)).
