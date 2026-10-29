"""Backward-compat re-export, everything lives in backpressure.queued_gate now."""

from backpressure.queued_gate import (
    AcquireStats as MultiAcquireStats,
)
from backpressure.queued_gate import (
    QueuedAcquireDeniedError,
    QueuedAcquireRequest,
)
from backpressure.queued_gate import (
    queued_acquire as queued_acquire_multi,
)
from backpressure.queued_gate import (
    queued_release as queued_release_multi,
)

__all__ = [
    "MultiAcquireStats",
    "QueuedAcquireDeniedError",
    "QueuedAcquireRequest",
    "queued_acquire_multi",
    "queued_release_multi",
]
