"""Internal local activities for Backpressure admission.

These are registered automatically by ``BackpressurePlugin`` and should not be
imported or called directly by user code.  The module exposes:

* The ``__bp_acquire`` / ``__bp_release`` / ``__bp_inline_run`` activity
  functions.
* The module-level state (``valkey_pool``, ``targets``, ``activity_target_map``,
  ``inline_activity_fns``) that the plugin's ``_run_context`` mutates around
  worker startup / shutdown.
* ``stats_from_dict``, a tiny helper that turns the dict shape returned by
  ``__bp_acquire`` back into an ``AcquireStats``.

Names are intentionally not prefixed with ``_`` here even though the module
itself is package-private: cross-module access from ``gate.py`` /
``plugin.py`` is part of the contract, and a leading underscore would just
trip private-usage type checks for no benefit.
"""

from __future__ import annotations

import inspect
import logging
import time
from typing import TYPE_CHECKING, Any

from temporalio import activity
from temporalio.common import _NoopMetricMeter  # type: ignore[attr-defined]
from temporalio.exceptions import ApplicationError
from valkey.exceptions import ValkeyError

from backpressure import QueuedAcquireRequest, queued_acquire, queued_release
from backpressure.queued_gate import AcquireStats, QueuedAcquireDeniedError, QueuedAcquireTimeoutError

if TYPE_CHECKING:
    from collections.abc import Callable

    from temporalio.common import MetricHistogram
    from valkey.asyncio import Valkey

    from backpressure.types import TargetConfig

__all__ = [
    "activity_target_map",
    "bp_acquire",
    "bp_inline_run",
    "bp_release",
    "inline_activity_fns",
    "stats_from_dict",
    "targets",
    "valkey_pool",
]

logger = logging.getLogger(__name__)

# Set by BackpressurePlugin._run_context before worker starts.
valkey_pool: Valkey | None = None
targets: dict[str, TargetConfig] = {}
# activity-type-name → target_id  (populated from plugin.activity_targets)
activity_target_map: dict[str, str] = {}
# activity-type-name → user activity function (populated when the plugin is
# constructed with `inline_activities`).  Only consulted by the inline-mode
# wrapper activity ``__bp_inline_run``, left empty in local-activity mode.
inline_activity_fns: dict[str, Callable[..., Any]] = {}

_DENIED_TYPE = "backpressure:denied"
_TIMEOUT_TYPE = "backpressure:queued_timeout"

# Errors that trigger the degraded-open fallback (invariant I3): Valkey
# unreachable, transport failure, or script error.  Admission is bypassed and
# the call proceeds ungated rather than failing the activity.
_FAIL_OPEN_ERRORS: tuple[type[BaseException], ...] = (ValkeyError, OSError)

# Pre-built error messages avoid Ruff TRY003/EM101/EM102 churn at the raise
# sites.  Targets that need to be parameterised get a small helper below.
_VALKEY_NOT_INITIALISED = "BackpressurePlugin Valkey connection not initialised"


def _unknown_target_msg(target_id: str) -> str:
    return f"BackpressurePlugin: unknown target {target_id!r}"


def _unknown_inline_activity_msg(activity_name: str) -> str:
    return (
        f"BackpressurePlugin: activity {activity_name!r} not registered for inline mode. "
        "Pass it via inline_activities= to BackpressurePlugin."
    )


def _coerce_resource_costs(raw: object) -> dict[str, float]:
    """Narrow ``payload['resource_costs']`` (typed as ``Any``) to a typed dict."""
    if not isinstance(raw, dict):
        return {}
    return {str(k): float(v) for k, v in raw.items()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]


def _coerce_activity_args(raw: object) -> list[Any]:
    """Coerce a Temporal payload list to a Python list."""
    if not isinstance(raw, list):
        return []
    return list(raw)  # pyright: ignore[reportUnknownArgumentType]


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------


def _make_histogram(name: str, description: str, unit: str) -> MetricHistogram:
    try:
        meter = activity.metric_meter()
    except RuntimeError:
        meter = _NoopMetricMeter()
    return meter.create_histogram(name, description=description, unit=unit)


def _eval_attrs(target_id: str, tenant: str, priority: int) -> dict[str, str]:
    return {"target": target_id, "tenant": tenant, "priority": str(priority)}


def _record_acquire_metrics(
    *,
    target_id: str,
    tenant: str,
    priority: int,
    wait_ms: int,
    polls: int,
) -> None:
    attrs = _eval_attrs(target_id, tenant, priority)
    _make_histogram(
        "backpressure_queued_acquire_wait_ms",
        description="Wall time blocked in queued_acquire before grant (milliseconds)",
        unit="ms",
    ).with_additional_attributes(attrs).record(wait_ms)
    _make_histogram(
        "backpressure_acquire_polls",
        description="Number of Valkey EVAL polls per queued_acquire (distribution)",
        unit="polls",
    ).with_additional_attributes(attrs).record(polls)


def _record_eval_poll(
    *,
    target_id: str,
    tenant: str,
    priority: int,
    eval_ms: int,
    granted: bool,
) -> None:
    attrs = _eval_attrs(target_id, tenant, priority)
    m_eval = _make_histogram(
        "backpressure_acquire_eval_ms",
        description="Latency of one Valkey EVAL roundtrip inside queued_acquire (milliseconds)",
        unit="ms",
    )
    label = "true" if granted else "false"
    m_eval.with_additional_attributes({**attrs, "granted": label}).record(eval_ms)


# ---------------------------------------------------------------------------
# __bp_acquire
# ---------------------------------------------------------------------------


@activity.defn(name="__bp_acquire")
async def bp_acquire(payload: dict[str, Any]) -> dict[str, Any]:
    """Run queued_acquire and return AcquireStats as a plain dict.

    Raises ``ApplicationError(type='backpressure:denied')`` or
    ``ApplicationError(type='backpressure:queued_timeout')`` on admission
    failure so the workflow-side retry loop in ``gated_activity`` can catch
    them with the same error types as before.
    """
    if valkey_pool is None:
        raise RuntimeError(_VALKEY_NOT_INITIALISED)

    target_id: str = str(payload["target_id"])
    target = targets.get(target_id)
    if target is None:
        raise RuntimeError(_unknown_target_msg(target_id))

    tenant: str = str(payload["tenant_id"])
    priority: int = int(payload["priority"])
    resource_costs = _coerce_resource_costs(payload.get("resource_costs"))
    request_id: str = str(payload["request_id"])

    def _record(eval_ms: int, granted: bool, _label: str = "") -> None:
        _record_eval_poll(
            target_id=target_id,
            tenant=tenant,
            priority=priority,
            eval_ms=eval_ms,
            granted=granted,
        )

    timeout_s: float = float(payload.get("timeout_s", 30.0))

    t_acquire = time.monotonic()
    try:
        stats = await queued_acquire(
            valkey=valkey_pool,
            target=target,
            request=QueuedAcquireRequest(
                tenant_id=tenant,
                priority=priority,
                resource_costs=resource_costs,
                request_id=request_id,
                timeout_s=timeout_s,
                blocking=True,
            ),
            record_poll=_record,
        )
    except QueuedAcquireDeniedError as exc:
        raise ApplicationError(
            str(exc),
            exc.retry_after_ms,
            type=_DENIED_TYPE,
            non_retryable=True,
        ) from exc
    except QueuedAcquireTimeoutError as exc:
        raise ApplicationError(
            str(exc),
            type=_TIMEOUT_TYPE,
            non_retryable=True,
        ) from exc
    except _FAIL_OPEN_ERRORS as exc:
        # Degraded open (I3): admission state is unavailable, so bypass the
        # gate and let the call proceed as if Backpressure were not installed.
        logger.warning(
            "backpressure degraded open: admission unavailable for target %s (%s); proceeding ungated",
            target_id,
            exc,
        )
        now_ms = int(time.time() * 1000)
        return {
            "wait_ms": round((time.monotonic() - t_acquire) * 1000),
            "polls": 0,
            "enqueued_at_ms": now_ms,
            "granted_at_ms": now_ms,
            "degraded": True,
        }

    wait_wall_ms = round((time.monotonic() - t_acquire) * 1000)
    _record_acquire_metrics(
        target_id=target_id,
        tenant=tenant,
        priority=priority,
        wait_ms=wait_wall_ms,
        polls=stats.polls,
    )

    return {
        "wait_ms": stats.wait_ms,
        "polls": stats.polls,
        "enqueued_at_ms": stats.enqueued_at_ms,
        "granted_at_ms": stats.granted_at_ms,
    }


# ---------------------------------------------------------------------------
# __bp_release
# ---------------------------------------------------------------------------


@activity.defn(name="__bp_release")
async def bp_release(payload: dict[str, Any]) -> None:
    """Run queued_release to refund a granted token when the user activity failed.

    Valkey errors are swallowed and logged so the original activity exception
    remains the surfaced failure (same behaviour as the old interceptor path).
    """
    if valkey_pool is None:
        logger.warning("BackpressurePlugin Valkey not initialised; skipping release")
        return

    target_id: str = str(payload["target_id"])
    target = targets.get(target_id)
    if target is None:
        logger.warning("BackpressurePlugin: unknown target %r; skipping release", target_id)
        return

    resource_costs = _coerce_resource_costs(payload.get("resource_costs"))

    try:
        await queued_release(
            valkey=valkey_pool,
            target=target,
            tenant_id=str(payload["tenant_id"]),
            resource_costs=resource_costs,
            priority=int(payload["priority"]),
        )
    except Exception:
        logger.exception(
            "backpressure queued_release failed for target %s tenant %s; ignoring",
            target_id,
            payload.get("tenant_id"),
        )


def stats_from_dict(d: dict[str, Any]) -> AcquireStats:
    """Reconstruct an ``AcquireStats`` from the dict returned by ``bp_acquire``."""
    return AcquireStats(
        wait_ms=int(d["wait_ms"]),
        polls=int(d["polls"]),
        enqueued_at_ms=int(d["enqueued_at_ms"]),
        granted_at_ms=int(d["granted_at_ms"]),
    )


# ---------------------------------------------------------------------------
# __bp_inline_run, single-activity admission + execution
#
# Used by ``gated_activity()`` when ``BACKPRESSURE_GATE_MODE=inline`` to fold
# the gate acquire and the user activity body into one regular activity. This
# saves one workflow-task / matching-service round-trip per request. Tradeoff:
# the regular activity slot is held during the gate wait, the same way the
# old ApplicationInterceptor path did. Acceptable when slots are not the
# bottleneck (typical eval shape: 1.4 of 1000 slots used) and worth it when
# scheduling round-trips dominate.
# ---------------------------------------------------------------------------


async def _inline_acquire(
    *,
    target: TargetConfig,
    target_id: str,
    tenant: str,
    priority: int,
    resource_costs: dict[str, float],
    request_id: str,
    timeout_s: float,
) -> tuple[AcquireStats, bool]:
    """Run the gate acquire portion of inline mode.

    Emits the same metrics as ``bp_acquire`` and wraps Valkey errors in
    ``ApplicationError`` of the types the workflow-side retry loop catches.
    Returns ``(stats, degraded)``; ``degraded`` is True when admission state
    was unavailable and the gate degraded open (I3) without debiting buckets.
    """
    if valkey_pool is None:  # invariant, caller checked this
        msg = "valkey_pool not initialized"
        raise RuntimeError(msg)

    def _record(eval_ms: int, granted: bool, _label: str = "") -> None:
        _record_eval_poll(
            target_id=target_id,
            tenant=tenant,
            priority=priority,
            eval_ms=eval_ms,
            granted=granted,
        )

    t_acquire = time.monotonic()
    try:
        stats = await queued_acquire(
            valkey=valkey_pool,
            target=target,
            request=QueuedAcquireRequest(
                tenant_id=tenant,
                priority=priority,
                resource_costs=resource_costs,
                request_id=request_id,
                timeout_s=timeout_s,
                blocking=True,
            ),
            record_poll=_record,
        )
    except QueuedAcquireDeniedError as exc:
        raise ApplicationError(
            str(exc),
            exc.retry_after_ms,
            type=_DENIED_TYPE,
            non_retryable=True,
        ) from exc
    except QueuedAcquireTimeoutError as exc:
        raise ApplicationError(
            str(exc),
            type=_TIMEOUT_TYPE,
            non_retryable=True,
        ) from exc
    except _FAIL_OPEN_ERRORS as exc:
        # Degraded open (I3): bypass admission, nothing was debited.
        logger.warning(
            "backpressure degraded open: admission unavailable for target %s (%s); proceeding ungated",
            target_id,
            exc,
        )
        now_ms = int(time.time() * 1000)
        degraded_stats = AcquireStats(
            wait_ms=round((time.monotonic() - t_acquire) * 1000),
            polls=0,
            enqueued_at_ms=now_ms,
            granted_at_ms=now_ms,
        )
        return degraded_stats, True

    # Override stats.wait_ms with the wall-clock acquire-block measurement so
    # downstream histograms agree with bp_acquire's behaviour.
    wait_wall_ms = round((time.monotonic() - t_acquire) * 1000)
    stats = AcquireStats(
        wait_ms=wait_wall_ms,
        polls=stats.polls,
        enqueued_at_ms=stats.enqueued_at_ms,
        granted_at_ms=stats.granted_at_ms,
    )
    _record_acquire_metrics(
        target_id=target_id,
        tenant=tenant,
        priority=priority,
        wait_ms=wait_wall_ms,
        polls=stats.polls,
    )
    return stats, False


async def _inline_release_best_effort(
    *,
    target: TargetConfig,
    target_id: str,
    tenant: str,
    priority: int,
    resource_costs: dict[str, float],
) -> None:
    """Best-effort refund of a granted token after the user activity raised."""
    if valkey_pool is None:
        return
    try:
        await queued_release(
            valkey=valkey_pool,
            target=target,
            tenant_id=tenant,
            resource_costs=resource_costs,
            priority=priority,
        )
    except Exception:
        logger.exception(
            "backpressure inline release failed for target %s tenant %s; ignoring",
            target_id,
            tenant,
        )


async def _reconcile_costs(
    *,
    target: TargetConfig,
    target_id: str,
    tenant: str,
    priority: int,
    estimated_costs: dict[str, float],
    result: object,
) -> None:
    """Adjust bucket state based on actual usage reported in the activity result.

    When the activity result contains a ``usage`` dict (as returned by mocklimit),
    compute the delta between estimated and actual per-dimension costs:
    - If estimated > actual: release the overestimate (refund excess tokens)
    - If estimated < actual: no action (we can't retroactively debit; the
      request already went through and the upstream counted it)

    This keeps the gate's internal bucket aligned with the upstream's actual
    consumption, preventing drift that would cause the TPM gate to be overly
    conservative (always over-debiting from estimates).
    """
    if not isinstance(result, dict):
        return
    usage_raw: Any = result.get("usage")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
    if not isinstance(usage_raw, dict):
        return
    usage: dict[str, Any] = {str(k): v for k, v in usage_raw.items()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
    if valkey_pool is None:
        return

    _reconcile_threshold = 0.5
    refund: dict[str, float] = {}
    debit: dict[str, float] = {}
    for dim, estimated in estimated_costs.items():
        actual = _resolve_actual_cost(usage, dim)
        if actual is None:
            continue
        delta = estimated - actual
        if delta > _reconcile_threshold:
            refund[dim] = delta
        elif delta < -_reconcile_threshold:
            debit[dim] = -delta

    if not refund and not debit:
        return

    logger.debug(
        "Reconciling costs for target=%s tenant=%s: refund=%s debit=%s",
        target_id,
        tenant,
        refund or None,
        debit or None,
    )
    # Single signed release: refunds are positive, debits negated. The Lua
    # release adds the signed cost per dimension (negative subtracts), so the
    # over/under-estimate reconciliation lands in one EVAL instead of two.
    # A dimension is mutually exclusive between refund and debit (delta is either
    # above +threshold or below -threshold), so the merge is unambiguous.
    signed: dict[str, float] = dict(refund)
    for dim, amount in debit.items():
        signed[dim] = -amount
    try:
        await queued_release(
            valkey=valkey_pool,
            target=target,
            tenant_id=tenant,
            resource_costs=signed,
            priority=priority,
        )
    except Exception:
        logger.debug(
            "Cost reconciliation release failed for target %s; ignoring",
            target_id,
            exc_info=True,
        )


def _resolve_actual_cost(usage: dict[str, Any], dim: str) -> float | None:
    """Extract the actual cost for a dimension from the usage dict.

    The usage dict from mocklimit has the shape:
        {"requests": 1, "tokens": 570, "inputs": {"requests": 1, "tokens": 120}, "outputs": {"tokens": 450}}

    Dimension names can be dotted (e.g. "tokens.input", "tokens.output") or
    plain (e.g. "requests", "tokens" for total).
    """
    if "." in dim:
        resource, component = dim.split(".", 1)
        if component == "input":
            inputs: dict[str, Any] = dict(usage.get("inputs") or {})
            if resource in inputs:
                return float(inputs[resource])
        elif component == "output":
            outputs: dict[str, Any] = dict(usage.get("outputs") or {})
            if resource in outputs:
                return float(outputs[resource])
        return None

    # Plain dimension: look up directly in totals
    if dim in usage:
        return float(usage[dim])
    return None


@activity.defn(name="__bp_inline_run")
async def bp_inline_run(payload: dict[str, Any]) -> dict[str, Any]:
    """Acquire the gate, run the user activity, return the merged result.

    Payload fields (set by ``gated_activity`` when in ``inline`` mode)::

        target_id        str
        tenant_id        str
        priority         int
        resource_costs   dict[str, float]
        request_id       str
        timeout_s        float
        activity_name    str, key into ``inline_activity_fns``
        activity_args    list, positional args for the user fn

    Returns the user-activity result dict augmented with queue stats fields
    (``queued_wait_ms``, ``queued_polls``, ``queue_enqueued_ms``,
    ``queue_granted_ms``).  If the user activity raises, the gate is best-
    effort released and the exception is re-raised.

    Raises ``ApplicationError(type=backpressure:denied | queued_timeout)``
    on admission failure so the caller's retry loop in ``gated_activity``
    can catch them.
    """
    if valkey_pool is None:
        raise RuntimeError(_VALKEY_NOT_INITIALISED)

    # Captured at the very top so ``activity_occupancy_ms`` (derived as
    # ``activity_returning_ms - activity_started_ms`` in the eval workflow)
    # reflects how long the regular Temporal activity slot was actually held.
    # In inline mode this *includes* the gate wait, in local-activity mode
    # the user activity overrides ``activity_started_ms`` itself so the field
    # captures only the provider call.
    inline_activity_started_ms = int(time.time() * 1000)

    target_id: str = str(payload["target_id"])
    target = targets.get(target_id)
    if target is None:
        raise RuntimeError(_unknown_target_msg(target_id))

    tenant: str = str(payload["tenant_id"])
    priority: int = int(payload["priority"])
    resource_costs = _coerce_resource_costs(payload.get("resource_costs"))
    request_id: str = str(payload["request_id"])
    timeout_s: float = float(payload.get("timeout_s", 30.0))
    activity_name: str = str(payload["activity_name"])
    activity_args = _coerce_activity_args(payload.get("activity_args"))

    fn = inline_activity_fns.get(activity_name)
    if fn is None:
        raise RuntimeError(_unknown_inline_activity_msg(activity_name))

    # 1) Gate acquire, same Lua/queued_acquire path as the local-activity mode.
    stats, degraded = await _inline_acquire(
        target=target,
        target_id=target_id,
        tenant=tenant,
        priority=priority,
        resource_costs=resource_costs,
        request_id=request_id,
        timeout_s=timeout_s,
    )

    # 2) Run the user activity body in-process.
    try:
        result = fn(*activity_args)
        if inspect.isawaitable(result):
            result = await result
    except BaseException:
        # Best-effort refund. Mirrors the workflow-side _RELEASE_LA path.
        # Skipped on a degraded-open grant: nothing was debited, so a refund
        # would credit tokens the bucket never lost.
        if not degraded:
            await _inline_release_best_effort(
                target=target,
                target_id=target_id,
                tenant=tenant,
                priority=priority,
                resource_costs=resource_costs,
            )
        raise

    # 2b) Reconcile: adjust bucket state based on actual usage from response.
    # Only meaningful for multi-constraint targets where token estimates can
    # diverge from actuals. Single-dim (RPM-only) targets skip this entirely
    # to preserve the proven single-dim gate behavior. Skipped on a
    # degraded-open grant for the same reason as the refund above.
    if len(target.constraints) > 1 and not degraded:
        await _reconcile_costs(
            target=target,
            target_id=target_id,
            tenant=tenant,
            priority=priority,
            estimated_costs=resource_costs,
            result=result,
        )

    # 3) Merge queue stats into the result so downstream code (e.g. the eval
    #    workflow's request_log) keeps seeing the same shape.  ``gate_mode``
    #    is fixed to ``"inline"`` here so the local-activity / inline split is
    #    visible per-request without consulting suite-level config.
    #
    #    ``activity_started_ms`` is overridden with the moment we entered this
    #    activity body, this way ``activity_occupancy_ms`` (computed by the
    #    eval workflow as ``activity_returning_ms - activity_started_ms``)
    #    captures the full slot-held duration, including the gate wait.
    #    ``activity_returning_ms`` is also pinned to "now" so a user fn that
    #    returned a stale timestamp (e.g. the mocklimit activity does
    #    ``int(time.time() * 1000)`` *before* the wrapper's post-processing)
    #    cannot accidentally clip the interval.
    extra: dict[str, Any] = {
        "queued_wait_ms": stats.wait_ms,
        "queued_polls": stats.polls,
        "queue_enqueued_ms": stats.enqueued_at_ms,
        "queue_granted_ms": stats.granted_at_ms,
        "gate_mode": "inline",
        "activity_started_ms": inline_activity_started_ms,
        "activity_returning_ms": int(time.time() * 1000),
    }
    if isinstance(result, dict):
        return {str(k): v for k, v in result.items()} | extra  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
    # Non-dict results: wrap so the workflow always gets a dict back.
    return {"value": result, **extra}
