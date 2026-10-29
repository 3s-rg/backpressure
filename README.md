# Backpressure

Async, multi-tenant, multi-dimensional rate limiting for Python and Go workers, backed by [Valkey](https://valkey.io/) (Redis-compatible).

Before each upstream call, workers acquire a permit from a shared Valkey admission gate. The gate enforces a global throughput cap, three priority classes with configurable refill shares, and weighted fair share among tenants that are actively competing. Waits happen inside each worker via `asyncio.sleep`. No proxy, no extra process.

## Packages

| Package | Registry | Source |
|---------|----------|--------|
| `backpressure` | PyPI | [`python/core/`](python/core) |
| `backpressure-temporal` | PyPI | [`python/temporal/`](python/temporal) |
| `github.com/stano45/backpressure/go` | Go module | [`go/`](go) |

```bash
pip install backpressure
pip install backpressure-temporal   # Temporal plugin
go get github.com/stano45/backpressure/go
```

Python and Go workers share the same Valkey keyspace and admission scripts, so they coordinate transparently on the same targets. Each package README covers its own usage: [`python/core/`](python/core), [`python/temporal/`](python/temporal), [`go/`](go). Runnable end-to-end setups live in [`python/examples/`](python/examples) and [`go/examples/`](go/examples).

## Development

```bash
just check             # format check, lint, type check, unit tests
just test              # unit tests only
just test-go           # go tests
just up                # start Valkey + mocklimit via Docker
just test-integration  # integration tests (requires just up)
just down              # stop Docker services
```

## Releasing

- **PyPI**: publish a GitHub release. The [publish workflow](.github/workflows/publish.yml) builds `backpressure` and `backpressure-temporal` and uploads both via PyPI trusted publishing.
- **Go**: tag `go/vX.Y.Z` and push the tag. The Go proxy picks it up from the repo.
