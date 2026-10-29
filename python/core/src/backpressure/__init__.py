"""Backpressure, Valkey-backed queued admission for Python workers."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from loguru import logger

from backpressure.costs import estimate_costs
from backpressure.loader import (
    ActivityCostConfig,
    BackpressureConfig,
    UpstreamConfig,
    load_config,
)
from backpressure.queued_gate import (
    AcquireStats,
    QueuedAcquireDeniedError,
    QueuedAcquireRequest,
    QueuedAcquireTimeoutError,
    queued_acquire,
    queued_release,
)
from backpressure.types import Constraint, TargetConfig

if TYPE_CHECKING:
    from typing import TextIO

__all__ = [
    "AcquireStats",
    "ActivityCostConfig",
    "BackpressureConfig",
    "Constraint",
    "QueuedAcquireDeniedError",
    "QueuedAcquireRequest",
    "QueuedAcquireTimeoutError",
    "TargetConfig",
    "UpstreamConfig",
    "configure_logging",
    "estimate_costs",
    "load_config",
    "queued_acquire",
    "queued_release",
]

logger.disable("backpressure")

_handler_id: int | None = None


def configure_logging(*, level: str = "DEBUG", sink: TextIO | None = None) -> int:
    """Enable backpressure log output.

    Adds a stderr handler filtered to ``backpressure`` messages at *level*.
    Repeated calls replace the previous handler instead of stacking.

    Parameters
    ----------
    level:
        Minimum level for backpressure messages (e.g. ``"TRACE"``, ``"DEBUG"``,
        ``"WARNING"``).
    sink:
        Writable stream to send output to.  Defaults to ``sys.stderr``.

    Returns
    -------
    int
        Handler id, pass to ``loguru.logger.remove()`` to tear down.

    """
    global _handler_id  # noqa: PLW0603
    logger.enable("backpressure")

    if _handler_id is not None:
        logger.remove(_handler_id)

    _handler_id = logger.add(
        sink or sys.stderr,
        level=level,
        filter="backpressure",
        format=(
            "<dim>{time:HH:mm:ss.SSS}</dim> | "
            "<lvl>{level:<8}</lvl> | "
            "<dim>{name}:{function}:{line}</dim>, "
            "<lvl>{message}</lvl>"
        ),
    )
    return _handler_id
