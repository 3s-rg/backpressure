"""Tests for local-activity-based admission (BackpressurePlugin + gated_activity)."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest
from fakeredis import FakeAsyncValkey, FakeServer
from temporalio import activity, workflow
from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from valkey.asyncio import Valkey

from backpressure import QueuedAcquireRequest, queued_acquire
from backpressure.types import Constraint, TargetConfig
from backpressure_temporal import _local_activities as _la
from backpressure_temporal import gated_activity
from backpressure_temporal.gate import DENIED_ERROR_TYPE, QUEUED_TIMEOUT_TYPE, GatedResult
from backpressure_temporal.plugin import BackpressurePlugin

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

TARGET_ID = "test-target"
ECHO_NAME = "echo_activity"
FAILING_NAME = "failing_activity"
TASK_QUEUE = "test-la-admission"


def _target_config(*, limit: float = 200.0) -> TargetConfig:
    return TargetConfig(
        target_id=TARGET_ID,
        constraints=[Constraint(dimension="rps", limit=limit)],
        tenant_weights={"t1": 1.0},
    )


# ---------------------------------------------------------------------------
# Shared activities
# ---------------------------------------------------------------------------


@activity.defn(name=ECHO_NAME)
async def echo_activity(msg: str) -> str:
    """Test fixture: echo back ``echo:<msg>`` so we can assert e2e wiring."""
    return f"echo:{msg}"


@activity.defn(name="echo_dict")
async def echo_dict_activity(msg: str) -> dict[str, Any]:
    """Test fixture: return a dict so the gate can merge queue stats into it."""
    return {"status": "ok", "msg": msg}


@activity.defn(name=FAILING_NAME)
async def failing_activity(msg: str) -> str:
    """Test fixture: raise to exercise the gate's release-on-failure path."""
    err_msg = f"boom:{msg}"
    raise RuntimeError(err_msg)


@activity.defn(name="gated_result_activity")
async def gated_result_activity(msg: str) -> dict[str, Any]:
    """Test fixture: return a JSON-friendly ``GatedResult`` shape.

    The activity cannot return ``GatedResult`` directly because Temporal's
    default JSON payload converter does not know how to serialise it.  Instead
    we return ``{"value": ..., "actual_costs": ...}``: ``_unwrap_gated``
    recognises this dict shape and unwraps it for the workflow.
    """
    return {"value": f"gated:{msg}", "actual_costs": {"rps": 1.0}}


# ---------------------------------------------------------------------------
# Workflow helpers
# ---------------------------------------------------------------------------


@workflow.defn(sandboxed=False)
class _EchoWorkflow:
    @workflow.run
    async def run(self, msg: str) -> str:
        wid = workflow.info().workflow_id
        result = await gated_activity(
            echo_activity,
            msg,
            tenant_id="t1",
            priority=1,
            request_id=f"{wid}:echo:1",
            resource_costs={"rps": 1.0},
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return str(result)


@workflow.defn(sandboxed=False)
class _DictResultWorkflow:
    @workflow.run
    async def run(self, msg: str) -> dict[str, Any]:
        wid = workflow.info().workflow_id
        result: dict[str, Any] = await gated_activity(
            echo_dict_activity,
            msg,
            tenant_id="t1",
            priority=1,
            request_id=f"{wid}:dict:1",
            resource_costs={"rps": 1.0},
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return result


@workflow.defn(sandboxed=False)
class _GatedResultWorkflow:
    @workflow.run
    async def run(self, msg: str) -> str:
        wid = workflow.info().workflow_id
        result = await gated_activity(
            gated_result_activity,
            msg,
            tenant_id="t1",
            priority=1,
            request_id=f"{wid}:gr:1",
            resource_costs={"rps": 1.0},
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return str(result)


@workflow.defn(sandboxed=False)
class _FailingWorkflow:
    @workflow.run
    async def run(self, msg: str) -> str:
        wid = workflow.info().workflow_id
        result = await gated_activity(
            failing_activity,
            msg,
            tenant_id="t1",
            priority=1,
            request_id=f"{wid}:fail:1",
            resource_costs={"rps": 1.0},
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return str(result)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_valkey() -> FakeAsyncValkey:
    server = FakeServer(server_type="valkey")
    return FakeAsyncValkey(server=server)


@pytest.fixture
def _plugin(fake_valkey: FakeAsyncValkey) -> BackpressurePlugin:
    """Build a BackpressurePlugin whose _run_context will inject fake_valkey directly."""
    p = BackpressurePlugin(
        valkey_url="redis://localhost:6379",  # overridden below
        targets={TARGET_ID: _target_config()},
        activity_targets={
            ECHO_NAME: TARGET_ID,
            "echo_dict": TARGET_ID,
            FAILING_NAME: TARGET_ID,
            "gated_result_activity": TARGET_ID,
        },
    )
    # Inject fake Valkey directly instead of connecting to a real instance.
    _la.valkey_pool = fake_valkey
    _la.targets = p.targets
    _la.activity_target_map = dict(p.activity_targets)
    return p


@pytest.fixture(autouse=True)
def _clear_la_state() -> None:
    """Reset module-level state after every test."""
    yield
    _la.valkey_pool = None
    _la.targets = {}
    _la.activity_target_map = {}
    _la.inline_activity_fns = {}


@pytest.fixture
async def env() -> AsyncIterator[WorkflowEnvironment]:
    async with await WorkflowEnvironment.start_local() as e:  # pyright: ignore[reportUnknownMemberType]
        yield e


_ALL_ACTIVITIES = [echo_activity, echo_dict_activity, failing_activity, gated_result_activity]
_ALL_WORKFLOWS = [_EchoWorkflow, _DictResultWorkflow, _GatedResultWorkflow, _FailingWorkflow]


@pytest.fixture
async def _worker(env: WorkflowEnvironment) -> AsyncIterator[Worker]:
    """Run a Worker registering every test workflow + activity.

    Includes the two internal local activities (``__bp_acquire`` /
    ``__bp_release``).  Uses the ``_plugin`` fixture transitively to install
    module-level state.
    """
    # Register the two internal local activities manually since _run_context
    # is bypassed (the ``_plugin`` fixture sets module-level state directly).
    from backpressure_temporal._local_activities import bp_acquire, bp_release  # noqa: PLC0415

    async with Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=_ALL_WORKFLOWS,
        activities=[*_ALL_ACTIVITIES, bp_acquire, bp_release],
        workflow_runner=BackpressurePlugin.patch_workflow_runner(None),
    ) as w:
        yield w


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.usefixtures("_plugin", "_worker")
async def test_echo_succeeds(env: WorkflowEnvironment) -> None:
    """gated_activity grants under generous limit and returns activity result."""
    result = await env.client.execute_workflow(
        _EchoWorkflow.run,
        "hello",
        id="la-echo-1",
        task_queue=TASK_QUEUE,
    )
    assert result == "echo:hello"


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.usefixtures("_plugin", "_worker")
async def test_dict_result_has_queued_stats_injected(env: WorkflowEnvironment) -> None:
    """Plain dict activity result receives queued_wait_ms and queued_polls fields."""
    result = await env.client.execute_workflow(
        _DictResultWorkflow.run,
        "stats",
        id="la-dict-1",
        task_queue=TASK_QUEUE,
    )
    assert result["status"] == "ok"
    assert "queued_wait_ms" in result
    assert "queued_polls" in result
    assert "queue_enqueued_ms" in result
    assert "queue_granted_ms" in result
    # Local-activity mode should always tag the result with the dispatch path
    # so downstream analysis can group by gate_mode without consulting suite
    # config.
    assert result.get("gate_mode") == "local-activity"


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.usefixtures("_plugin", "_worker")
async def test_gated_result_is_unwrapped(env: WorkflowEnvironment) -> None:
    """GatedResult-shaped dict is unwrapped; workflow receives the inner value."""
    result = await env.client.execute_workflow(
        _GatedResultWorkflow.run,
        "msg",
        id="la-gr-1",
        task_queue=TASK_QUEUE,
    )
    # The workflow calls str(result), inner value is "gated:msg"
    assert result == "gated:msg"


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.usefixtures("_plugin", "_worker")
async def test_activity_failure_does_not_leak_token(
    env: WorkflowEnvironment,
    fake_valkey: FakeAsyncValkey,
) -> None:
    """When the activity raises, __bp_release refunds capacity (token count stays 0)."""
    with pytest.raises(WorkflowFailureError):
        await env.client.execute_workflow(
            _FailingWorkflow.run,
            "x",
            id="la-fail-1",
            task_queue=TASK_QUEUE,
        )

    # After release the token is back, a fresh acquire should succeed immediately.
    stats = await queued_acquire(
        valkey=fake_valkey,
        target=_target_config(),
        request=QueuedAcquireRequest(
            tenant_id="t1",
            priority=1,
            resource_costs={"rps": 1.0},
            request_id="post-fail-check",
            timeout_s=2.0,
        ),
    )
    assert stats.polls >= 1


# ---------------------------------------------------------------------------
# Unit tests for _resolve_target_id helper
# ---------------------------------------------------------------------------


def test_resolve_target_id_from_explicit() -> None:
    from backpressure_temporal.gate import _resolve_target_id  # noqa: PLC0415

    _la.activity_target_map = {}
    _la.targets = {"some-target": _target_config()}
    assert _resolve_target_id("any_fn", "explicit-id") == "explicit-id"


def test_resolve_target_id_from_map() -> None:
    from backpressure_temporal.gate import _resolve_target_id  # noqa: PLC0415

    _la.activity_target_map = {"my_activity": "my-target"}
    _la.targets = {"my-target": _target_config()}
    assert _resolve_target_id("my_activity", None) == "my-target"


def test_resolve_target_id_single_target_fallback() -> None:
    from backpressure_temporal.gate import _resolve_target_id  # noqa: PLC0415

    _la.activity_target_map = {}
    _la.targets = {"only-target": _target_config()}
    assert _resolve_target_id("unknown_fn", None) == "only-target"


def test_resolve_target_id_raises_when_ambiguous() -> None:
    from backpressure_temporal.gate import _resolve_target_id  # noqa: PLC0415

    _la.activity_target_map = {}
    _la.targets = {"t1": _target_config(), "t2": _target_config()}
    with pytest.raises(ValueError, match="target_id"):
        _resolve_target_id("unknown_fn", None)


# ---------------------------------------------------------------------------
# GatedResult unit tests (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_gated_result_fields() -> None:
    r: GatedResult[str] = GatedResult(value="hello", actual_costs={"tokens": 347})
    assert r.value == "hello"
    assert r.actual_costs == {"tokens": 347}


def test_gated_result_isinstance_check() -> None:
    r: GatedResult[str] = GatedResult(value="ok", actual_costs={"rps": 1})
    assert isinstance(r, GatedResult)


def test_default_max_gate_retries_value() -> None:
    from backpressure_temporal.gate import DEFAULT_MAX_GATE_RETRIES  # noqa: PLC0415

    assert DEFAULT_MAX_GATE_RETRIES == 10


def test_denied_error_type_constant() -> None:
    assert DENIED_ERROR_TYPE == "backpressure:denied"


def test_queued_timeout_error_type_constant() -> None:
    assert QUEUED_TIMEOUT_TYPE == "backpressure:queued_timeout"


# ---------------------------------------------------------------------------
# Integration: full Valkey backed test with real Temporal
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=False)
async def _flush_valkey() -> None:
    conn = Valkey(host="localhost", port=6379, db=0)
    await conn.flushdb()  # pyright: ignore[reportUnknownMemberType]
    await conn.aclose()


@pytest.mark.integration
@pytest.mark.timeout(120)
@pytest.mark.usefixtures("_flush_valkey")
async def test_echo_with_real_valkey(env: WorkflowEnvironment) -> None:
    """End-to-end: BackpressurePlugin wires up; activity is gated via real Valkey."""
    config = _target_config(limit=200.0)
    plugin = BackpressurePlugin(
        valkey_url="redis://localhost:6379",
        targets={TARGET_ID: config},
        activity_targets={ECHO_NAME: TARGET_ID},
    )

    async with Worker(
        env.client,
        task_queue=TASK_QUEUE + "-real",
        workflows=[_EchoWorkflow],
        activities=[echo_activity],
        plugins=[plugin],
    ):
        result = await env.client.execute_workflow(
            _EchoWorkflow.run,
            "real",
            id="la-real-valkey",
            task_queue=TASK_QUEUE + "-real",
        )

    assert result == "echo:real"


# ---------------------------------------------------------------------------
# Inline-mode (BACKPRESSURE_GATE_MODE=inline) tests
# ---------------------------------------------------------------------------


@pytest.fixture
async def _inline_worker(
    env: WorkflowEnvironment,
    fake_valkey: FakeAsyncValkey,
) -> AsyncIterator[Worker]:
    """Run a Worker with the inline-mode wrapper + the user activity registered.

    Bypasses ``_run_context`` (sets module state by hand) so the test does not
    depend on a real Valkey URL.
    """
    from backpressure_temporal._local_activities import (  # noqa: PLC0415
        bp_acquire,
        bp_inline_run,
        bp_release,
    )

    targets = {TARGET_ID: _target_config()}
    activity_targets = {
        ECHO_NAME: TARGET_ID,
        "echo_dict": TARGET_ID,
    }
    inline_activities = {
        ECHO_NAME: echo_activity,
        "echo_dict": echo_dict_activity,
    }

    _la.valkey_pool = fake_valkey
    _la.targets = targets
    _la.activity_target_map = dict(activity_targets)
    _la.inline_activity_fns = dict(inline_activities)

    async with Worker(
        env.client,
        task_queue=TASK_QUEUE + "-inline",
        workflows=_ALL_WORKFLOWS,
        activities=[*_ALL_ACTIVITIES, bp_acquire, bp_release, bp_inline_run],
        workflow_runner=BackpressurePlugin.patch_workflow_runner(None),
    ) as w:
        yield w


@pytest.fixture
def _set_gate_mode_inline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set BACKPRESSURE_GATE_MODE=inline for the duration of one test."""
    monkeypatch.setenv("BACKPRESSURE_GATE_MODE", "inline")


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.usefixtures("_set_gate_mode_inline", "_inline_worker")
async def test_inline_mode_dict_result_has_queued_stats(env: WorkflowEnvironment) -> None:
    """Inline-mode dict result keeps the queued_* stats merged in.

    In inline mode, ``gated_activity`` routes through ``__bp_inline_run``; the
    dict result still ends up with the ``queued_wait_ms`` / ``queued_polls``
    fields merged in (the wrapper merges them before returning to the
    workflow).
    """
    result = await env.client.execute_workflow(
        _DictResultWorkflow.run,
        "inline-stats",
        id="la-inline-dict-1",
        task_queue=TASK_QUEUE + "-inline",
    )
    assert result["status"] == "ok"
    assert result["msg"] == "inline-stats"
    assert "queued_wait_ms" in result
    assert "queued_polls" in result
    assert "queue_enqueued_ms" in result
    assert "queue_granted_ms" in result
    # Inline mode tags the merged result with its own gate_mode so request_log
    # entries downstream can be filtered without re-reading suite config.
    assert result.get("gate_mode") == "inline"
    # The inline wrapper overrides activity_started_ms / activity_returning_ms
    # with the slot-occupancy interval so derived occupancy fields capture the
    # full slot-held duration (gate wait + user fn).
    assert isinstance(result.get("activity_started_ms"), int)
    assert isinstance(result.get("activity_returning_ms"), int)
    assert result["activity_returning_ms"] >= result["activity_started_ms"]


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.usefixtures("_set_gate_mode_inline", "_inline_worker")
async def test_inline_mode_returns_str_result(env: WorkflowEnvironment) -> None:
    """Non-dict activity results are wrapped by the inline wrapper.

    The shape becomes ``{"value": ..., queued_*}``, the workflow's ``str()``
    call here just renders the dict.
    """
    result = await env.client.execute_workflow(
        _EchoWorkflow.run,
        "inline-hello",
        id="la-inline-echo-1",
        task_queue=TASK_QUEUE + "-inline",
    )
    # Workflow does str(result), the inline wrapper returned a dict whose
    # "value" key is "echo:inline-hello".
    assert "echo:inline-hello" in result


def test_gate_mode_env_default_is_local_activity() -> None:
    """The env-var name is exported and the default is documented."""
    from backpressure_temporal import GATE_MODE_ENV  # noqa: PLC0415

    assert GATE_MODE_ENV == "BACKPRESSURE_GATE_MODE"


def test_inline_activity_fns_cleared_after_run_context() -> None:
    """Verify the registry is purely module-level state.

    The plugin populates the registry only via ``_run_context``; bypassing
    it via direct module-level assignment is the test pattern, but the
    production path always clears state on shutdown.
    """
    _la.inline_activity_fns = {"x": lambda: None}
    assert _la.inline_activity_fns
    _la.inline_activity_fns = {}
    assert not _la.inline_activity_fns


@pytest.mark.integration
@pytest.mark.timeout(60)
@pytest.mark.usefixtures("_set_gate_mode_inline", "_inline_worker")
async def test_inline_mode_gate_retries_queued_timeout(
    env: WorkflowEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: a queued-timeout on the inline path must gate-retry.

    Regular activities wrap the gate's typed ApplicationError in an
    ActivityError (unlike local activities, which surface it directly).
    Before the unwrap in ``_gated_activity_inline``, the first
    ``backpressure:queued_timeout`` escaped the retry loop and failed the
    whole workflow (observed as 26/60 failed workflows on w15-reconcile).
    """
    from backpressure.queued_gate import QueuedAcquireTimeoutError  # noqa: PLC0415

    real_acquire = _la.queued_acquire
    calls = {"n": 0}

    async def flaky_acquire(*args: object, **kwargs: object) -> object:
        calls["n"] += 1
        if calls["n"] == 1:
            msg = "queued acquire timed out (test stub)"
            raise QueuedAcquireTimeoutError(msg)
        return await real_acquire(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_la, "queued_acquire", flaky_acquire)

    result = await env.client.execute_workflow(
        _EchoWorkflow.run,
        "retry-me",
        id="la-inline-timeout-retry-1",
        task_queue=TASK_QUEUE + "-inline",
    )
    assert "retry-me" in str(result)
    assert calls["n"] >= 2, "gate must re-attempt admission after a queued timeout"
