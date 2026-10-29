"""Tests for the degraded-open fallback (invariant I3).

When Valkey is unreachable (connection refused, transport failure, script
error), admission must degrade open: the acquire returns a grant marked
``degraded`` and the call proceeds ungated, instead of failing the activity.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Worker
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from backpressure.types import Constraint, TargetConfig
from backpressure_temporal import _local_activities as _la
from backpressure_temporal import gated_activity
from backpressure_temporal._local_activities import _inline_acquire, bp_acquire, bp_release

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from valkey.asyncio import Valkey

TARGET_ID = "test-target"
ECHO_NAME = "echo_activity"
TASK_QUEUE = "test-fail-open"


class _DownValkey:
    """Valkey stand-in that refuses every command with a connection error."""

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        async def _fail(*_args: object, **_kwargs: object) -> object:
            msg = "connection refused (test stub)"
            raise ValkeyConnectionError(msg)

        return _fail


def _target_config() -> TargetConfig:
    return TargetConfig(
        target_id=TARGET_ID,
        constraints=[Constraint(dimension="rps", limit=200.0)],
        tenant_weights={"t1": 1.0},
    )


def _acquire_payload() -> dict[str, Any]:
    return {
        "target_id": TARGET_ID,
        "tenant_id": "t1",
        "priority": 1,
        "resource_costs": {"rps": 1.0},
        "request_id": "req-1",
        "timeout_s": 1.0,
    }


@pytest.fixture(autouse=True)
def _la_state() -> Any:  # noqa: ANN401
    """Install a down Valkey + target map, and reset module state afterwards."""
    _la.valkey_pool = cast("Valkey", _DownValkey())
    _la.targets = {TARGET_ID: _target_config()}
    _la.activity_target_map = {ECHO_NAME: TARGET_ID}
    yield
    _la.valkey_pool = None
    _la.targets = {}
    _la.activity_target_map = {}
    _la.inline_activity_fns = {}


# ---------------------------------------------------------------------------
# Activity-level
# ---------------------------------------------------------------------------


async def test_bp_acquire_degrades_open() -> None:
    """A Valkey connection error must yield a degraded grant, not an error."""
    env = ActivityEnvironment()
    stats_dict = await env.run(bp_acquire, _acquire_payload())
    assert stats_dict["degraded"] is True
    assert stats_dict["polls"] == 0
    assert stats_dict["granted_at_ms"] > 0


async def test_inline_acquire_degrades_open() -> None:
    """The inline acquire must return (stats, degraded=True) on Valkey errors."""
    env = ActivityEnvironment()

    async def _probe() -> tuple[int, bool]:
        stats, degraded = await _inline_acquire(
            target=_la.targets[TARGET_ID],
            target_id=TARGET_ID,
            tenant="t1",
            priority=1,
            resource_costs={"rps": 1.0},
            request_id="req-1",
            timeout_s=1.0,
        )
        return stats.polls, degraded

    probe = activity.defn(name="_probe_inline_acquire")(_probe)
    polls, degraded = await env.run(probe)
    assert degraded is True
    assert polls == 0


# ---------------------------------------------------------------------------
# End-to-end through gated_activity
# ---------------------------------------------------------------------------


@activity.defn(name=ECHO_NAME)
async def echo_activity(msg: str) -> str:
    """Test fixture: echo back ``echo:<msg>`` so we can assert e2e wiring."""
    return f"echo:{msg}"


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


@pytest.fixture
async def env() -> AsyncIterator[WorkflowEnvironment]:
    async with await WorkflowEnvironment.start_local() as e:  # pyright: ignore[reportUnknownMemberType]
        yield e


async def test_gated_activity_degrades_open_end_to_end(env: WorkflowEnvironment) -> None:
    """With Valkey down, the gated activity must still execute and return."""
    async with Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[_EchoWorkflow],
        activities=[echo_activity, bp_acquire, bp_release],
    ):
        result = await env.client.execute_workflow(
            _EchoWorkflow.run,
            "hi",
            id="wf-fail-open-1",
            task_queue=TASK_QUEUE,
        )
    assert result == "echo:hi"
