# Go examples

Full runnable setups for both packages. All commands run from `go/`.

## Prerequisites

```bash
# from the repo root: start Valkey (the gate's backing store)
docker compose up -d valkey
```

## 1. Admission gate only (`github.com/stano45/backpressure/go`)

Two tenants with weights 2:1 hammer a 5 req/s target from 4 concurrent callers each. After 10 seconds the grant counts skew toward the heavier tenant.

```bash
go run ./examples/gate
```

## 2. Temporal integration (`.../go/temporal`)

Additionally requires a local Temporal dev server:

```bash
temporal server start-dev
```

Terminal A, the worker (gate + gated activity + workflow, 5 req/s target shared by two tenants):

```bash
go run ./examples/worker
```

Terminal B, start a workflow and print its result:

```bash
go run ./examples/workflow
```

Set `BACKPRESSURE_GATE_MODE=inline` on the worker to fold admission and the activity body into a single dispatch (see [`../README.md`](../README.md)).
