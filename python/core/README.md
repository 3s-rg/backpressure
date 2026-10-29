# backpressure

Async, multi-tenant, multi-dimensional rate limiting for Python workers, backed by [Valkey](https://valkey.io/) (Redis-compatible).

Before each upstream call, workers acquire a permit from a shared Valkey admission gate. The gate enforces a global throughput cap, three priority classes with configurable refill shares, and weighted fair share among tenants that are actively competing. Multi-dimensional targets (for example requests/s plus tokens/s) are enforced atomically with dominant-resource fairness. Waits happen inside each worker via `asyncio.sleep`. No proxy, no extra process.

```bash
pip install backpressure
```

```python
from backpressure import QueuedAcquireRequest, queued_acquire

req = QueuedAcquireRequest(tenant_id="team-a", priority=1)
stats = await queued_acquire(valkey=valkey, target=config, request=req)
# call the rate-limited upstream here
```

For the Temporal integration, install [`backpressure-temporal`](https://pypi.org/project/backpressure-temporal/).

Source and issues: [github.com/stano45/backpressure](https://github.com/stano45/backpressure)
