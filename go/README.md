# Backpressure Go

Valkey-backed queued admission for outbound requests, with a drop-in Temporal worker integration. The Go equivalent of the Python [`backpressure`](../python/core) and [`backpressure-temporal`](../python/temporal) packages: all implementations share the same Valkey keyspace and admission scripts, so workers in any language coordinate on the same targets.

## Install

```bash
go get github.com/stano45/backpressure/go
```

Releases of the Go module are tagged `go/vX.Y.Z` (nested-module convention).

Two Lua admission scripts back the gate:

- single-dimension (N=1): [`lua/admission_token_buckets.lua`](lua/admission_token_buckets.lua)
- multi-dimension DRF (N>=2): [`lua/admission_token_buckets_multi.lua`](lua/admission_token_buckets_multi.lua)

`QueuedAcquire`/`QueuedRelease` enforce **all** dimensions in `TargetConfig.Constraints` atomically and dispatch on the constraint count: N=1 uses the single-dim script (keys `bpq:{T}:tb:global` / `bpq:{T}:tb:pri`), N>=2 uses the multi-dim DRF script with per-dimension keys (`bpq:{T}:tb:global:{dim}` / `bpq:{T}:tb:pri:{dim}`). Workers that share a Valkey instance and a `target_id` coordinate transparently, including on the same multi-dim target, provided all of them configure the constraints in the same order.

## Layout

```
go/
  *.go            # core gate at the module root: QueuedAcquire, QueuedRelease, types
  lua/            # Lua admission scripts
  temporal/       # Temporal Go SDK integration: GatedActivity + activities
  examples/       # minimal worker + workflow demo
```

Module path: `github.com/stano45/backpressure/go`. Requires Go 1.26.

## Core gate (no Temporal required)

```go
import (
    "context"
    bp "github.com/stano45/backpressure/go"
    "github.com/valkey-io/valkey-go"
)

client, _ := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
defer client.Close()

target := bp.TargetConfig{
    TargetID:      "llm-api",
    Constraints:   []bp.Constraint{{Dimension: "rps", Limit: 100}},
    TenantWeights: map[string]float64{"team_a": 1.0},
}

stats, err := bp.QueuedAcquire(ctx, client, target, bp.QueuedAcquireRequest{
    TenantID:      "team_a",
    Priority:      1,
    ResourceCosts: map[string]float64{"rps": 1.0},
    RequestID:     "req-123",
    TimeoutS:      30,
    Blocking:      true,
}, nil)
// ... call upstream ...
// on failure:
_ = bp.QueuedRelease(ctx, client, target, "team_a", map[string]float64{"rps": 1.0}, 1)
```

`QueuedAcquire` returns either an `AcquireStats`, a `*bp.DeniedError` (non-blocking mode), or a `*bp.TimeoutError`. Pass any `context.Context` for cancellation.

## Temporal worker

```go
import (
    "github.com/valkey-io/valkey-go"
    "go.temporal.io/sdk/client"
    "go.temporal.io/sdk/worker"

    bptemporal "github.com/stano45/backpressure/go/temporal"
    bp "github.com/stano45/backpressure/go"
)

func main() {
    vk, _ := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
    defer vk.Close()

    gate := bptemporal.NewGate(vk, bptemporal.GateOptions{
        Targets: map[string]bp.TargetConfig{
            "llm-api": {
                TargetID:    "llm-api",
                Constraints: []bp.Constraint{{Dimension: "rps", Limit: 100}},
            },
        },
        ActivityTargets: map[string]string{
            "CallLLM": "llm-api", // activity type name -> target
        },
        // Required only when using inline gate mode:
        InlineActivities: map[string]any{"CallLLM": CallLLM},
    })

    c, _ := client.Dial(client.Options{})
    defer c.Close()
    w := worker.New(c, "my-task-queue", worker.Options{})

    bptemporal.RegisterActivities(w, gate)         // __bp_acquire/__bp_release/__bp_inline_run
    w.RegisterActivity(CallLLM)                    // your activity
    w.RegisterWorkflow(MyWorkflow)

    _ = w.Run(worker.InterruptCh())
}
```

## Workflow usage

```go
func MyWorkflow(ctx workflow.Context, input MyInput) (MyOutput, error) {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
    })

    var out MyOutput
    err := bptemporal.GatedActivity(ctx, gate, bptemporal.GateParams{
        TenantID:      input.TenantID,
        Priority:      1,
        RequestID:     bptemporal.MakeRequestID(ctx, 1, 1),
        ResourceCosts: map[string]float64{"rps": 1.0},
    }, "CallLLM", &out, input)
    return out, err
}
```

`gate` is normally a process-global pointer to the `*bptemporal.Gate` constructed in `main`. The workflow file only needs to import it.

Switch modes per worker with `BACKPRESSURE_GATE_MODE=inline` (default `local-activity`).

## Configuration file (YAML/JSON)

The same config file works for Python and Go workers:

```yaml
targets:
  llm-api:
    constraints:
      - { dimension: rps, limit: 100 }
    tenant_weights: { team_a: 1.0, team_b: 2.0 }
    priority_minimums: { "1": 0.5, "2": 0.3, "3": 0.2 }

activity_targets:
  CallLLM: llm-api
```

```go
gate, _ := bptemporal.NewGateFromConfig(vk, "config.yaml", map[string]any{
    "CallLLM": CallLLM,
})
```

## Environment variables

Read at every call.

| Variable | Default | Purpose |
| --- | --- | --- |
| `QUEUED_GATE_BURST_SECONDS` | `1.0` | Burst window in seconds |
| `QUEUED_GATE_GLOBAL_BURST_CAP_MULT` | `1.20` | Global cap multiplier |
| `QUEUED_GATE_REGISTRY_IDLE_MS` | `10000` | Idle-tenant eviction horizon |
| `QUEUED_GATE_ACQUIRE_TIMEOUT_S` | `30.0` | Per-attempt acquire timeout |
| `BACKPRESSURE_GATE_MODE` | `local-activity` | `local-activity` or `inline` |

## Notes

- Workers sharing a Valkey instance share a target's buckets, regardless of language.
- Multi-dimension targets (N>=2 constraints) use the DRF script. The per-tenant share-bucket stores a positional cost vector, so a shared multi-dim target requires the **same constraint ordering** in every worker's config. `AcquireStats.DominantDim` (grant) and `DeniedError.BindingLabel` (deny) carry the gate's dimension label, both are empty for single-dim targets.
- `RequestID` must be unique per admission attempt and stable across replay. Use `bptemporal.MakeRequestID(ctx, seq, attempt)` inside workflows.
- `time.Now().UnixMilli()` drives every refill, backward clock jumps are clamped to zero.

## Tests

The core admission tests hit a real Valkey on `localhost:6379` (the repo's [`docker-compose.yml`](../docker-compose.yml) binding):

```bash
docker compose up -d valkey
cd go && go test ./...
```

Override with `BACKPRESSURE_TEST_VALKEY=host:port` if Valkey runs elsewhere, tests are skipped if it is unreachable.
