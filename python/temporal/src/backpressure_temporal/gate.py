"""Public helpers for Backpressure-gated Temporal activities."""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

if TYPE_CHECKING:
    from backpressure.queued_gate import AcquireStats

__all__ = [
    "DEFAULT_MAX_GATE_RETRIES",
    "DENIED_ERROR_TYPE",
    "GATE_MODE_ENV",
    "QUEUED_TIMEOUT_TYPE",
    "GatedResult",
    "gated_activity",
]

logger = logging.getLogger(__name__)

DENIED_ERROR_TYPE = "backpressure:denied"
QUEUED_TIMEOUT_TYPE = "backpressure:queued_timeout"
DEFAULT_MAX_GATE_RETRIES = 10
DEFAULT_GATE_RETRY_SLEEP_S = 0.05

# Name of the env var that selects the gate mode.  Read at every
# ``gated_activity()`` invocation so a worker restart can flip the switch
# without code changes.  Values:
#   "local-activity" (default), current behaviour: gate runs in
#     ``__bp_acquire`` local activity, then the user activity is dispatched as
#     a regular activity.  Two workflow tasks per request, one matching-service
#     dispatch.
#   "inline", gate + user activity execute inside one regular activity
#     (``__bp_inline_run``).  One workflow task per request, one matching-
#     service dispatch.  Requires the user activity to be registered with
#     ``BackpressurePlugin(inline_activities=...)``.
GATE_MODE_ENV = "BACKPRESSURE_GATE_MODE"
_GATE_MODE_LOCAL_ACTIVITY = "local-activity"
_GATE_MODE_INLINE = "inline"

# Retry policy for the acquire local activity: no automatic retries, the
# workflow-side loop owns retry logic via new request_id suffixes.
_ACQUIRE_LA_RETRY = RetryPolicy(maximum_attempts=1)
# Same idea for the inline regular activity: workflow-side loop handles
# admission retries, activity-level retries would just multiply the gate work.
_INLINE_RETRY = RetryPolicy(maximum_attempts=1)
# Release is best-effort, retry up to 3 times on transient failures.
_RELEASE_LA_RETRY = RetryPolicy(maximum_attempts=3)
_RELEASE_LA_TIMEOUT = timedelta(seconds=10)


# ---------------------------------------------------------------------------
# GatedResult
# ---------------------------------------------------------------------------


class GatedResult[T]:
    """Wrapper returned by activities that report actual resource consumption.

    ``actual_costs`` is reserved for future reconciliation.  The workflow
    receives the inner ``value`` after the gate injects queue timing fields
    into dict payloads.
    """

    def __init__(self, value: T, actual_costs: dict[str, float]) -> None:
        """Construct a GatedResult wrapping ``value`` with reported ``actual_costs``."""
        self.value = value
        self.actual_costs = actual_costs


# ---------------------------------------------------------------------------
# Internal helpers (moved from interceptor.py)
# ---------------------------------------------------------------------------


class _UnwrappedGate:
    __slots__ = ("actual_costs", "value")

    def __init__(self, value: object, actual_costs: dict[str, float]) -> None:
        self.value = value
        self.actual_costs = actual_costs


def _unwrap_gated(result: object) -> _UnwrappedGate | None:
    """Extract gate fields from *result* if it looks like a ``GatedResult``.

    Recognised shapes:

    * an in-process ``GatedResult`` instance (returned only when the activity
      executes locally without going through Temporal's data converter);
    * a duck-typed object exposing ``value`` and ``actual_costs`` attributes;
    * a plain ``dict`` with ``"value"`` and ``"actual_costs"`` keys, this is
      the JSON-friendly shape activities use, since ``GatedResult`` itself is
      not natively JSON-serialisable by Temporal's default payload converter.
    """
    if isinstance(result, GatedResult):
        return _UnwrappedGate(cast("object", result.value), result.actual_costs)
    if (
        isinstance(result, dict)
        and "value" in result
        and "actual_costs" in result
        and isinstance(result["actual_costs"], dict)
    ):
        d = cast("dict[str, object]", result)
        return _UnwrappedGate(d["value"], cast("dict[str, float]", d["actual_costs"]))
    # ``result`` is now ``object``, ``getattr`` widens to ``Any`` which would
    # leak partially-unknown typing back to callers, pin it to ``object``.
    obj = cast("object", result)
    costs: object = getattr(obj, "actual_costs", None)
    if costs is not None and isinstance(costs, dict) and hasattr(obj, "value"):
        return _UnwrappedGate(
            cast("object", getattr(obj, "value", None)),
            cast("dict[str, float]", costs),
        )
    return None


def _inject_queued_stats(
    result: object,
    stats: AcquireStats,
    *,
    gate_mode: str = _GATE_MODE_LOCAL_ACTIVITY,
) -> object:
    """Attach queue timing fields and the dispatching gate mode into the result.

    The ``gate_mode`` label distinguishes which dispatch path a given gated
    call took.  Downstream analysis (per-request request_log entries, suite
    aggregates) uses it to compare local-activity vs inline performance
    directly without having to cross-reference suite-level config.
    """
    extra = {
        "queued_wait_ms": stats.wait_ms,
        "queued_polls": stats.polls,
        "queue_enqueued_ms": stats.enqueued_at_ms,
        "queue_granted_ms": stats.granted_at_ms,
        "gate_mode": gate_mode,
    }
    unwrapped = _unwrap_gated(result)
    if unwrapped is not None:
        if isinstance(unwrapped.value, dict):
            merged = {**cast("dict[str, object]", unwrapped.value), **extra}
            return GatedResult(value=merged, actual_costs=unwrapped.actual_costs)
        return result
    if isinstance(result, dict):
        return {**cast("dict[str, object]", result), **extra}
    return result


# ---------------------------------------------------------------------------
# gated_activity
# ---------------------------------------------------------------------------


async def gated_activity(  # noqa: PLR0913
    activity_fn: Any,  # noqa: ANN401
    *args: Any,  # noqa: ANN401
    tenant_id: str,
    priority: int,
    request_id: str,
    target_id: str | None = None,
    resource_costs: dict[str, float] | None = None,
    max_gate_retries: int | None = DEFAULT_MAX_GATE_RETRIES,
    gate_retry_sleep_s: float = DEFAULT_GATE_RETRY_SLEEP_S,
    **execute_kwargs: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Execute an activity after queued admission.

    Two modes, selected at call time by the ``BACKPRESSURE_GATE_MODE`` env var
    (read inside the workflow's worker process, flipping it requires a
    worker restart):

    ``local-activity`` (default)
        Admission (``queued_acquire``) runs as a Temporal local activity
        (``__bp_acquire``) inside the worker process; the user activity is
        then dispatched as a regular activity through the matching service.
        The regular activity slot is held only for the duration of the user
        work, not during the Valkey queue poll.

    ``inline``
        Admission and the user activity body run together inside one regular
        activity (``__bp_inline_run``). One workflow task per request, one
        matching-service round-trip; the regular activity slot is held during
        both the gate wait and the user work. Requires the user activity to
        be registered with ``BackpressurePlugin(inline_activities=…)``.

    ``target_id`` is optional when the plugin was configured with
    ``activity_targets`` that maps ``activity_fn``'s name to a target.  Pass
    it explicitly when calling an activity whose name is not in
    ``activity_targets``, or when you want to override the default.

    ``max_gate_retries`` caps denial + queued-timeout retries combined
    (``None`` means unbounded, wall-clock bound from workflow ``run_timeout``).

    Usage inside a workflow::

        result = await gated_activity(
            "call_llm",
            prompt,
            tenant_id="team_a",
            priority=1,
            request_id="wf-id:seq:attempt",
            resource_costs={"tokens": 500},
            start_to_close_timeout=timedelta(seconds=30),
        )
    """
    # Resolve target_id: explicit arg > activity_target_map lookup > error.
    resolved_target_id = _resolve_target_id(activity_fn, target_id)
    fn_name = _activity_name(activity_fn)
    mode = os.environ.get(GATE_MODE_ENV, _GATE_MODE_LOCAL_ACTIVITY).strip().lower()

    if mode == _GATE_MODE_INLINE:
        return await _gated_activity_inline(
            fn_name=fn_name,
            activity_args=list(args),
            target_id=resolved_target_id,
            tenant_id=tenant_id,
            priority=priority,
            request_id=request_id,
            resource_costs=resource_costs or {},
            max_gate_retries=max_gate_retries,
            gate_retry_sleep_s=gate_retry_sleep_s,
            execute_kwargs=execute_kwargs,
        )

    # Default: local-activity mode.
    return await _gated_activity_local(
        activity_fn=activity_fn,
        activity_args=args,
        target_id=resolved_target_id,
        tenant_id=tenant_id,
        priority=priority,
        request_id=request_id,
        resource_costs=resource_costs or {},
        max_gate_retries=max_gate_retries,
        gate_retry_sleep_s=gate_retry_sleep_s,
        execute_kwargs=execute_kwargs,
    )


async def _gated_activity_local(  # noqa: PLR0913
    *,
    activity_fn: Any,  # noqa: ANN401
    activity_args: tuple[Any, ...],
    target_id: str,
    tenant_id: str,
    priority: int,
    request_id: str,
    resource_costs: dict[str, float],
    max_gate_retries: int | None,
    gate_retry_sleep_s: float,
    execute_kwargs: dict[str, Any],
) -> Any:  # noqa: ANN401
    acquire_timeout_s = float(os.environ.get("QUEUED_GATE_ACQUIRE_TIMEOUT_S", "30.0"))
    acquire_la_timeout = timedelta(seconds=acquire_timeout_s + 5)

    gate_retries = 0
    attempt_request_id = request_id

    while True:
        acquire_payload = {
            "target_id": target_id,
            "tenant_id": tenant_id,
            "priority": priority,
            "resource_costs": resource_costs,
            "request_id": attempt_request_id,
            "timeout_s": acquire_timeout_s,
        }

        try:
            stats_dict: dict[str, Any] = await workflow.execute_local_activity(
                "__bp_acquire",
                acquire_payload,
                start_to_close_timeout=acquire_la_timeout,
                retry_policy=_ACQUIRE_LA_RETRY,
            )
        except ApplicationError as exc:
            retry = _handle_gate_retry(
                exc=exc,
                request_id=request_id,
                gate_retries=gate_retries,
                max_gate_retries=max_gate_retries,
                gate_retry_sleep_s=gate_retry_sleep_s,
            )
            if retry is None:
                raise
            gate_retries, attempt_request_id, sleep_s = retry
            await workflow.sleep(sleep_s)
            continue

        # Admission granted, run the user activity.
        # A degraded-open grant (I3) debited nothing, so the failure-path
        # refund below must be skipped to avoid crediting undebited cost.
        degraded = bool(stats_dict.get("degraded", False))
        release_payload = {
            "target_id": target_id,
            "tenant_id": tenant_id,
            "priority": priority,
            "resource_costs": resource_costs,
        }
        try:
            result = await workflow.execute_activity(activity_fn, *activity_args, **execute_kwargs)
        except BaseException:
            if not degraded:
                try:
                    await workflow.execute_local_activity(
                        "__bp_release",
                        release_payload,
                        start_to_close_timeout=_RELEASE_LA_TIMEOUT,
                        retry_policy=_RELEASE_LA_RETRY,
                    )
                except Exception:  # noqa: BLE001, release is best-effort; logging suffices
                    logger.debug("__bp_release local activity failed; ignoring", exc_info=True)
            raise

        # Inject AcquireStats into result dict for analysis.
        from backpressure_temporal._local_activities import stats_from_dict  # noqa: PLC0415

        stats = stats_from_dict(stats_dict)
        out = _inject_queued_stats(result, stats, gate_mode=_GATE_MODE_LOCAL_ACTIVITY)
        uw = _unwrap_gated(out)
        return uw.value if uw is not None else out


async def _gated_activity_inline(  # noqa: PLR0913
    *,
    fn_name: str,
    activity_args: list[Any],
    target_id: str,
    tenant_id: str,
    priority: int,
    request_id: str,
    resource_costs: dict[str, float],
    max_gate_retries: int | None,
    gate_retry_sleep_s: float,
    execute_kwargs: dict[str, Any],
) -> Any:  # noqa: ANN401
    """Inline-mode body: schedule ``__bp_inline_run`` once per request.

    The inline activity returns the user-result dict already merged with the
    queue stats fields, so we just unwrap a possible ``GatedResult`` shape and
    return.
    """
    acquire_timeout_s = float(os.environ.get("QUEUED_GATE_ACQUIRE_TIMEOUT_S", "30.0"))
    gate_retries = 0
    attempt_request_id = request_id

    while True:
        payload = {
            "target_id": target_id,
            "tenant_id": tenant_id,
            "priority": priority,
            "resource_costs": resource_costs,
            "request_id": attempt_request_id,
            "timeout_s": acquire_timeout_s,
            "activity_name": fn_name,
            "activity_args": list(activity_args),
        }
        # The inline wrapper needs at least the gate timeout plus the user
        # activity's worst-case duration. We honour the caller's
        # start_to_close_timeout for the user activity by adding a buffer.
        user_sts = execute_kwargs.get("start_to_close_timeout")
        if isinstance(user_sts, timedelta):
            inline_timeout = user_sts + timedelta(seconds=acquire_timeout_s + 5)
        else:
            inline_timeout = timedelta(seconds=acquire_timeout_s + 60)

        try:
            result = await workflow.execute_activity(
                "__bp_inline_run",
                payload,
                start_to_close_timeout=inline_timeout,
                schedule_to_start_timeout=execute_kwargs.get("schedule_to_start_timeout"),
                retry_policy=_INLINE_RETRY,
            )
        except ActivityError as exc:
            # Regular activities wrap the gate's typed ApplicationError in an
            # ActivityError (unlike local activities, which surface it
            # directly), unwrap before deciding on a gate retry.
            cause = exc.cause
            if not isinstance(cause, ApplicationError):
                raise
            retry = _handle_gate_retry(
                exc=cause,
                request_id=request_id,
                gate_retries=gate_retries,
                max_gate_retries=max_gate_retries,
                gate_retry_sleep_s=gate_retry_sleep_s,
            )
            if retry is None:
                raise
            gate_retries, attempt_request_id, sleep_s = retry
            await workflow.sleep(sleep_s)
            continue

        uw = _unwrap_gated(result)
        return uw.value if uw is not None else result


def _handle_gate_retry(
    *,
    exc: ApplicationError,
    request_id: str,
    gate_retries: int,
    max_gate_retries: int | None,
    gate_retry_sleep_s: float,
) -> tuple[int, str, float] | None:
    """Decide whether to retry after a gate denial or queued-timeout.

    Returns ``(new_gate_retries, attempt_request_id, sleep_s)`` if the caller
    should retry after sleeping that long, or ``None`` if the exception
    should be re-raised.
    """
    if exc.type == DENIED_ERROR_TYPE:
        gate_retries += 1
        if max_gate_retries is not None and gate_retries > max_gate_retries:
            return None
        delay_ms = int(exc.details[0]) if exc.details else int(DEFAULT_GATE_RETRY_SLEEP_S * 1000)
        attempt_request_id = f"{request_id}:g{gate_retries}"
        logger.debug(
            "gate denied (retry %d/%s), sleeping %dms before retry",
            gate_retries,
            "unbounded" if max_gate_retries is None else max_gate_retries,
            delay_ms,
        )
        return (gate_retries, attempt_request_id, delay_ms / 1000.0)

    if exc.type == QUEUED_TIMEOUT_TYPE:
        gate_retries += 1
        if max_gate_retries is not None and gate_retries > max_gate_retries:
            return None
        attempt_request_id = f"{request_id}:g{gate_retries}"
        logger.debug(
            "queued acquire timed out (retry %d/%s), sleeping %.3fs",
            gate_retries,
            "unbounded" if max_gate_retries is None else max_gate_retries,
            gate_retry_sleep_s,
        )
        return (gate_retries, attempt_request_id, gate_retry_sleep_s)

    return None


def _activity_name(activity_fn: Any) -> str:  # noqa: ANN401
    """Resolve the registered Temporal activity name for ``activity_fn``."""
    if isinstance(activity_fn, str):
        return activity_fn
    defn = getattr(activity_fn, "__temporal_activity_definition", None)
    if defn is None:
        return getattr(activity_fn, "__name__", str(activity_fn))
    return getattr(defn, "name", str(defn))


def _resolve_target_id(activity_fn: Any, explicit: str | None) -> str:  # noqa: ANN401
    """Return the target_id to use for *activity_fn*.

    Priority: explicit kwarg > activity_target_map lookup > single-target
    shortcut > error.
    """
    if explicit is not None:
        return explicit

    # Lazy import to avoid circular dependency at module load time.
    from backpressure_temporal import _local_activities  # noqa: PLC0415

    fn_name = _activity_name(activity_fn)

    target_id = _local_activities.activity_target_map.get(fn_name)
    if target_id is not None:
        return target_id

    # Fallback: if only one target is registered use it.
    if len(_local_activities.targets) == 1:
        return next(iter(_local_activities.targets))

    msg = (
        f"gated_activity: cannot determine target_id for activity {fn_name!r}. "
        "Pass target_id= explicitly or configure activity_targets in BackpressurePlugin."
    )
    raise ValueError(msg)
