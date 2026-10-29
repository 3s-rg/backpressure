"""Atomic Valkey admission: global cap + per-priority refill + per-(tenant, priority) weighted fair share.

Enforces all dimensions in ``TargetConfig.constraints`` in a single atomic
EVAL and applies a Dominant Resource Fairness (DRF) criterion at L3
(per-tenant, per-priority).

Properties:
    - Atomic across dims: a grant debits all dims in one Lua EVAL.
    - DRF L3: per-tenant share-bucket refills at the tenant's weighted fair
      share of normalized share units; cost-per-request is the maximum
      ``cost_d / limit_d`` over dims.
    - Per-dim L1/L2: hard global cap per dim, work-conserving cross-priority
      yield per dim.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from valkey.exceptions import NoScriptError

from backpressure.loader import get_lua_script
from backpressure.types import TargetConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    from valkey.asyncio import Valkey

_DEFAULT_BURST_SECONDS = 1.0
_DEFAULT_GLOBAL_BURST_CAP_MULT = 1.20
_DEFAULT_IDLE_MS = 10_000
_DEFAULT_PRI_IDLE_MS = 2_000
_DEFAULT_PRIORITY_SHARES: dict[int, float] = {1: 0.5, 2: 0.3, 3: 0.2}
logger = logging.getLogger(__name__)

_ADMISSION_SCRIPT_SINGLE = get_lua_script("admission_token_buckets")
_ADMISSION_SCRIPT_MULTI = get_lua_script("admission_token_buckets_multi")

# Cached SHA1 digests, one per script text. Sending EVALSHA + the 40-char digest
# instead of the full multi-KB script body on every poll is the per-poll network
# win, the body is shipped at most once per server via the EVAL fallback below.
_SCRIPT_SHAS: dict[str, str] = {}


def _script_sha(name: str, text: str) -> str:
    sha = _SCRIPT_SHAS.get(name)
    if sha is None:
        sha = hashlib.sha1(text.encode("utf-8")).hexdigest()  # noqa: S324 - Valkey EVALSHA digest, not security
        _SCRIPT_SHAS[name] = sha
    return sha


def _is_noscript_error(exc: BaseException) -> bool:
    """Return whether *exc* is a NOSCRIPT error from any client (valkey, or redis-based fakes)."""
    if isinstance(exc, NoScriptError):
        return True
    # fakeredis raises redis.exceptions.NoScriptError, a distinct class from
    # valkey's, match by name/message so the EVAL fallback still fires in tests.
    return type(exc).__name__ == "NoScriptError" or "NOSCRIPT" in str(exc).upper()


async def _eval_script(
    client: Valkey,
    name: str,
    text: str,
    keys: list[str],
    argv: list[str],
) -> list[object]:
    """Run a script via EVALSHA, falling back to EVAL (which caches it) on NOSCRIPT."""
    sha = _script_sha(name, text)
    try:
        result: Any = await client.evalsha(sha, len(keys), *keys, *argv)  # pyright: ignore[reportGeneralTypeIssues,reportUnknownVariableType]
    except Exception as exc:
        if not _is_noscript_error(exc):
            raise
        result = await client.eval(text, len(keys), *keys, *argv)  # pyright: ignore[reportGeneralTypeIssues,reportUnknownVariableType]
    return list(result) if result is not None else []  # pyright: ignore[reportUnknownArgumentType]


@dataclass(frozen=True)
class AcquireStats:
    """Runtime diagnostics for a queued acquire."""

    wait_ms: int
    polls: int
    enqueued_at_ms: int
    granted_at_ms: int
    dominant_dim: str = ""
    """Dominant dimension reported by the gate at grant time (empty for N=1)."""


@dataclass(frozen=True)
class QueuedAcquireRequest:
    """Inputs for a queued acquire operation."""

    tenant_id: str
    priority: int
    resource_costs: dict[str, float]
    request_id: str
    timeout_s: float
    #: If True (default), sleep and retry inside the same task until *timeout_s*.
    #: If False, a capacity miss raises :class:`QueuedAcquireDeniedError` immediately.
    blocking: bool = True


class QueuedAcquireTimeoutError(TimeoutError):
    """Raised when a blocking acquire does not receive a permit in time."""


class QueuedAcquireDeniedError(Exception):
    """Non-blocking acquire could not debit capacity; retry after *retry_after_ms*."""

    binding_label: str

    def __init__(self, retry_after_ms: int) -> None:
        super().__init__(f"admission denied; retry after {retry_after_ms} ms")
        self.retry_after_ms = int(retry_after_ms)
        self.binding_label = ""


def _decode(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _tb_reg_key(target_id: str) -> str:
    return f"bpq:{target_id}:tb:reg"


def _tb_tp_key(target_id: str) -> str:
    return f"bpq:{target_id}:tb:tp"


def _tb_global_key(target_id: str, dim: str = "") -> str:
    if dim:
        return f"bpq:{target_id}:tb:global:{dim}"
    return f"bpq:{target_id}:tb:global"


def _tb_pri_key(target_id: str, dim: str = "") -> str:
    if dim:
        return f"bpq:{target_id}:tb:pri:{dim}"
    return f"bpq:{target_id}:tb:pri"


def _tb_act_key(target_id: str, priority: int) -> str:
    """Per-priority active-tenant sorted set (member=tenant, score=last_used_ms)."""
    return f"bpq:{target_id}:tb:act:{priority}"


def _tb_agg_key(target_id: str, priority: int) -> str:
    """Per-priority incremental aggregate hash (sum_w, denom_d, cfg_ver, rebuild_ms)."""
    return f"bpq:{target_id}:tb:agg:{priority}"


def _tb_con_key(target_id: str, priority: int) -> str:
    """Per-priority contribution hash (member=tenant -> stored cost vector).

    Lets the multi-dim DRF gate subtract a stale member's denominator
    contribution without re-reading the (possibly already swept) tp hash. Unused
    by the single-dim script, whose fair-share denominator is weight-only.
    """
    return f"bpq:{target_id}:tb:con:{priority}"


def _priority_shares(target: TargetConfig) -> tuple[float, float, float]:
    pm = target.priority_minimums or _DEFAULT_PRIORITY_SHARES
    s1 = float(pm.get(1, _DEFAULT_PRIORITY_SHARES[1]))
    s2 = float(pm.get(2, _DEFAULT_PRIORITY_SHARES[2]))
    s3 = float(pm.get(3, _DEFAULT_PRIORITY_SHARES[3]))
    total = s1 + s2 + s3
    if total <= 0:
        return (1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0)
    return (s1 / total, s2 / total, s3 / total)


def _tenant_weight(target: TargetConfig, tenant_id: str) -> float:
    if not target.tenant_weights:
        return float(target.default_weight)
    return float(target.tenant_weights.get(tenant_id, target.default_weight))


def _tenant_weights_payload(target: TargetConfig) -> tuple[str, float]:
    weights = {str(k): float(v) for k, v in (target.tenant_weights or {}).items()}
    return json.dumps(weights, separators=(",", ":")), float(target.default_weight)


def _cfg_version(parts: list[object]) -> str:
    """Fingerprint of the config that determines the per-priority aggregate.

    When any of these inputs change (tenant weights, dim limits/shares, dim
    count, idle windows), the stored ``sum_w``/``denom_d`` are no longer valid
    and the Lua triggers a full rebuild from the tp hash. Keep this in sync with
    every field the aggregate sum depends on.
    """
    payload = "|".join(str(p) for p in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()  # noqa: S324 - cache key, not security


def build_admission_call(
    target: TargetConfig,
    *,
    op: str,
    tenant_id: str,
    priority: int,
    resource_costs: dict[str, float],
    now_ms: int,
    burst_seconds: float,
    global_burst_mult: float,
    idle_ms: int,
    pri_idle_ms: int,
) -> tuple[str, str, list[str], list[str]]:
    """Build the (script_name, script_text, KEYS, ARGV) for one admission EVAL.

    Pure and side-effect free, so callers (and the differential test) can run
    the resulting payload against any script text without re-deriving the layout.
    Dispatches to the single-dim script for N=1 targets and the multi-dim DRF
    script for N>=2.
    """
    if not target.constraints:
        msg = "queued acquire requires at least one target constraint"
        raise ValueError(msg)
    s1, s2, s3 = _priority_shares(target)
    my_w = _tenant_weight(target, tenant_id)
    weights_json, default_w = _tenant_weights_payload(target)

    if len(target.constraints) == 1:
        c = target.constraints[0]
        cost = float(
            resource_costs.get(c.dimension, c.default_cost if c.default_cost is not None else 1.0),
        )
        keys = [
            _tb_global_key(target.target_id),
            _tb_pri_key(target.target_id),
            _tb_tp_key(target.target_id),
            _tb_reg_key(target.target_id),
            _tb_act_key(target.target_id, int(priority)),
            _tb_agg_key(target.target_id, int(priority)),
        ]
        cfg_ver = _cfg_version(
            ["1", weights_json, default_w, c.limit, s1, s2, s3, idle_ms, int(pri_idle_ms)],
        )
        argv: list[str] = [
            op,
            tenant_id,
            str(int(priority)),
            f"{cost}",
            str(now_ms),
            f"{c.limit}",
            f"{burst_seconds}",
            f"{global_burst_mult}",
            str(idle_ms),
            f"{s1}",
            f"{s2}",
            f"{s3}",
            f"{my_w}",
            weights_json,
            f"{default_w}",
            str(int(pri_idle_ms)),
            cfg_ver,
        ]
        return "admission_token_buckets", _ADMISSION_SCRIPT_SINGLE, keys, argv

    # Multi-dimension path (N >= 2)
    keys = [_tb_reg_key(target.target_id), _tb_tp_key(target.target_id)]
    for c in target.constraints:
        keys.append(_tb_global_key(target.target_id, c.dimension))
        keys.append(_tb_pri_key(target.target_id, c.dimension))
    keys.append(_tb_act_key(target.target_id, int(priority)))
    keys.append(_tb_agg_key(target.target_id, int(priority)))
    keys.append(_tb_con_key(target.target_id, int(priority)))

    argv = [
        op,
        tenant_id,
        str(int(priority)),
        str(now_ms),
        str(len(target.constraints)),
        f"{my_w}",
        weights_json,
        f"{default_w}",
        str(int(idle_ms)),
        str(int(pri_idle_ms)),
        f"{burst_seconds}",
        f"{global_burst_mult}",
    ]
    # cfg_ver fingerprints every input the per-priority aggregate depends on, a
    # mismatch forces a full rebuild from the tp hash (see _cfg_version).
    cfg_parts: list[object] = [str(len(target.constraints)), weights_json, default_w, idle_ms, int(pri_idle_ms)]
    for c in target.constraints:
        cost = float(
            resource_costs.get(c.dimension, c.default_cost if c.default_cost is not None else 1.0),
        )
        cfg_parts.extend((c.dimension, c.limit, s1, s2, s3))
        argv.extend(
            (
                c.dimension,
                f"{c.limit}",
                f"{cost}",
                f"{s1}",
                f"{s2}",
                f"{s3}",
            ),
        )
    argv.append(_cfg_version(cfg_parts))

    return "admission_token_buckets_multi", _ADMISSION_SCRIPT_MULTI, keys, argv


async def _eval_admission(
    client: Valkey,
    target: TargetConfig,
    *,
    op: str,
    tenant_id: str,
    priority: int,
    resource_costs: dict[str, float],
    now_ms: int,
    burst_seconds: float,
    global_burst_mult: float,
    idle_ms: int,
    pri_idle_ms: int,
) -> list[object]:
    """Build KEYS/ARGV and invoke the admission Lua script via EVALSHA."""
    name, text, keys, argv = build_admission_call(
        target,
        op=op,
        tenant_id=tenant_id,
        priority=priority,
        resource_costs=resource_costs,
        now_ms=now_ms,
        burst_seconds=burst_seconds,
        global_burst_mult=global_burst_mult,
        idle_ms=idle_ms,
        pri_idle_ms=pri_idle_ms,
    )
    return await _eval_script(
        client,
        name,
        text,
        keys,
        argv,
    )


def _parse(raw: list[object]) -> tuple[bool, int, str]:
    if len(raw) < 2:
        msg = f"admission script returned unexpected: {raw!r}"
        raise RuntimeError(msg)
    st = int(float(_decode(raw[0])))
    second = int(float(_decode(raw[1])))
    label = _decode(raw[2]) if len(raw) >= 3 else ""
    return st == 1, second, label


async def queued_acquire(
    *,
    valkey: Valkey,
    target: TargetConfig,
    request: QueuedAcquireRequest,
    record_poll: Callable[[int, bool, str], None] | None = None,
) -> AcquireStats:
    """Attempt admission under global + priority + per-tenant token buckets.

    Enforces all dimensions in ``target.constraints`` atomically.
    N=1 is the single-resource special case (RPM-only targets).

    * **blocking=True** (default): on miss, sleeps and retries until *timeout_s*.
    * **blocking=False**: on miss, raises :class:`QueuedAcquireDeniedError` immediately.

    *record_poll*, if supplied, is invoked once per Valkey EVAL with
    ``(eval_ms, granted, label)`` where ``label`` is the dominant dim on grant
    or ``"<dim>:global" | "<dim>:pri" | "share"`` on deny.
    """
    burst_seconds = float(os.environ.get("QUEUED_GATE_BURST_SECONDS", str(_DEFAULT_BURST_SECONDS)))
    g_mult = float(
        os.environ.get("QUEUED_GATE_GLOBAL_BURST_CAP_MULT", str(_DEFAULT_GLOBAL_BURST_CAP_MULT)),
    )
    idle_ms = int(os.environ.get("QUEUED_GATE_REGISTRY_IDLE_MS", str(_DEFAULT_IDLE_MS)))
    pri_idle_ms = int(os.environ.get("QUEUED_GATE_PRI_IDLE_MS", str(_DEFAULT_PRI_IDLE_MS)))

    start = time.monotonic()
    start_ms = int(time.time() * 1000)
    polls = 0

    while True:
        polls += 1
        now_ms = int(time.time() * 1000)
        t_eval = time.monotonic()
        raw = await _eval_admission(
            valkey,
            target,
            op="acquire",
            tenant_id=request.tenant_id,
            priority=request.priority,
            resource_costs=request.resource_costs,
            now_ms=now_ms,
            burst_seconds=burst_seconds,
            global_burst_mult=g_mult,
            idle_ms=idle_ms,
            pri_idle_ms=pri_idle_ms,
        )
        eval_ms = round((time.monotonic() - t_eval) * 1000)
        granted, second, label = _parse(raw)
        if record_poll is not None:
            try:
                record_poll(eval_ms, granted, label)
            except Exception:
                logger.debug("record_poll raised; ignoring", exc_info=True)
        if granted:
            return AcquireStats(
                wait_ms=round((time.monotonic() - start) * 1000),
                polls=polls,
                enqueued_at_ms=start_ms,
                granted_at_ms=second,
                dominant_dim=label,
            )

        retry_after_ms = max(1, second)
        if not request.blocking:
            err = QueuedAcquireDeniedError(retry_after_ms)
            err.binding_label = label
            raise err

        if time.monotonic() - start >= request.timeout_s:
            msg = f"queued acquire timed out for request {request.request_id}"
            raise QueuedAcquireTimeoutError(msg)

        await asyncio.sleep(retry_after_ms / 1000.0)


async def queued_release(
    *,
    valkey: Valkey,
    target: TargetConfig,
    tenant_id: str,
    resource_costs: dict[str, float],
    priority: int = 1,
) -> None:
    """Refund capacity reserved by a successful grant.

    Best-effort single round-trip; no lock required.
    """
    burst_seconds = float(os.environ.get("QUEUED_GATE_BURST_SECONDS", str(_DEFAULT_BURST_SECONDS)))
    g_mult = float(
        os.environ.get("QUEUED_GATE_GLOBAL_BURST_CAP_MULT", str(_DEFAULT_GLOBAL_BURST_CAP_MULT)),
    )
    idle_ms = int(os.environ.get("QUEUED_GATE_REGISTRY_IDLE_MS", str(_DEFAULT_IDLE_MS)))
    pri_idle_ms = int(os.environ.get("QUEUED_GATE_PRI_IDLE_MS", str(_DEFAULT_PRI_IDLE_MS)))

    now_ms = int(time.time() * 1000)
    await _eval_admission(
        valkey,
        target,
        op="release",
        tenant_id=tenant_id,
        priority=priority,
        resource_costs=resource_costs,
        now_ms=now_ms,
        burst_seconds=burst_seconds,
        global_burst_mult=g_mult,
        idle_ms=idle_ms,
        pri_idle_ms=pri_idle_ms,
    )


# Backward-compatible aliases
queued_acquire_multi = queued_acquire
queued_release_multi = queued_release
MultiAcquireStats = AcquireStats
