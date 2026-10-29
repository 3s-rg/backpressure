"""Tests for BackpressurePlugin construction, config loading, and wiring."""

from __future__ import annotations

import json
import tempfile
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
import yaml
from temporalio import activity, workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
from valkey.asyncio import Valkey

from backpressure.types import Constraint, TargetConfig
from backpressure_temporal import gated_activity
from backpressure_temporal.plugin import BackpressurePlugin

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

TASK_QUEUE = "test-plugin"
TARGET_ID = "test-target"
ACTIVITY_NAME = "plugin_echo"


@activity.defn(name=ACTIVITY_NAME)
async def _echo_activity(msg: str) -> str:
    return f"echo:{msg}"


@workflow.defn(sandboxed=False)
class _PluginWorkflow:
    @workflow.run
    async def run(self, msg: str) -> str:
        wid = workflow.info().workflow_id
        return await gated_activity(
            _echo_activity,
            msg,
            tenant_id="t1",
            priority=1,
            request_id=f"{wid}:plugin-echo",
            resource_costs={"rps": 1.0},
            start_to_close_timeout=timedelta(seconds=10),
        )


def test_default_construction() -> None:
    """Plugin can be created with defaults."""
    plugin = BackpressurePlugin()
    assert plugin.name() == "backpressure.RateLimiter"
    assert plugin.targets == {}
    assert plugin.activity_targets == {}


def test_construction_with_targets() -> None:
    """Plugin stores targets and activity_targets."""
    config = TargetConfig(
        target_id="openai",
        constraints=[Constraint(dimension="rps", limit=100)],
    )
    plugin = BackpressurePlugin(
        targets={"openai": config},
        activity_targets={"call_llm": "openai"},
    )
    assert "openai" in plugin.targets
    assert plugin.activity_targets == {"call_llm": "openai"}


def test_from_config_json() -> None:
    """Plugin loads from a JSON config file."""
    data = {
        "targets": {
            "openai": {
                "constraints": [
                    {"dimension": "tokens", "limit": 100000},
                    {"dimension": "rps", "limit": 100},
                ],
                "tenant_weights": {"team_a": 2.0, "team_b": 1.0},
            },
        },
        "activity_targets": {
            "call_llm": "openai",
            "embed_text": "openai",
        },
    }
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        json.dump(data, f)
        f.flush()
        plugin = BackpressurePlugin.from_config(f.name)

    assert "openai" in plugin.targets
    cfg = plugin.targets["openai"]
    assert cfg.target_id == "openai"
    assert len(cfg.constraints) == 2
    assert cfg.constraints[0].dimension == "tokens"
    assert cfg.constraints[1].dimension == "rps"
    assert cfg.tenant_weights == {"team_a": 2.0, "team_b": 1.0}
    assert plugin.activity_targets == {"call_llm": "openai", "embed_text": "openai"}


def test_from_config_yaml() -> None:
    """Plugin loads from a YAML config file."""
    data = {
        "targets": {
            "anthropic": {
                "constraints": [
                    {"dimension": "rps", "limit": 50},
                ],
                "tenant_weights": {"alpha": 1.0},
            },
        },
        "activity_targets": {
            "call_claude": "anthropic",
        },
    }
    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        yaml.dump(data, f)
        f.flush()
        plugin = BackpressurePlugin.from_config(f.name)

    assert "anthropic" in plugin.targets
    cfg = plugin.targets["anthropic"]
    assert cfg.target_id == "anthropic"
    assert plugin.activity_targets == {"call_claude": "anthropic"}


def test_from_config_target_id_defaults_to_key() -> None:
    """When target_id is omitted, it defaults to the dict key."""
    data = {
        "targets": {
            "my_api": {
                "constraints": [
                    {"dimension": "rps", "limit": 10},
                ],
            },
        },
        "activity_targets": {},
    }
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        json.dump(data, f)
        f.flush()
        plugin = BackpressurePlugin.from_config(f.name)

    assert plugin.targets["my_api"].target_id == "my_api"


def test_patch_workflow_runner_none() -> None:
    """When runner is None, creates a SandboxedWorkflowRunner with passthroughs."""
    runner = BackpressurePlugin.patch_workflow_runner(None)
    assert isinstance(runner, SandboxedWorkflowRunner)
    assert "backpressure" in runner.restrictions.passthrough_modules
    assert "backpressure_temporal" in runner.restrictions.passthrough_modules


def test_patch_workflow_runner_existing_sandbox() -> None:
    """When runner is a SandboxedWorkflowRunner, adds passthroughs."""
    original = SandboxedWorkflowRunner()
    assert "backpressure" not in original.restrictions.passthrough_modules

    patched = BackpressurePlugin.patch_workflow_runner(original)
    assert isinstance(patched, SandboxedWorkflowRunner)
    assert "backpressure" in patched.restrictions.passthrough_modules
    assert "backpressure_temporal" in patched.restrictions.passthrough_modules


# ---------------------------------------------------------------------------
# Integration: plugin wires interceptor through real Temporal worker
# ---------------------------------------------------------------------------


@pytest.fixture
async def _flush_valkey() -> None:  # pyright: ignore[reportUnusedFunction]
    conn = Valkey(host="localhost", port=6379, db=0)
    await conn.flushdb()  # pyright: ignore[reportUnknownMemberType]
    await conn.aclose()


@pytest.fixture
async def env() -> AsyncIterator[WorkflowEnvironment]:
    """Start an ephemeral Temporal dev server for the test."""
    async with await WorkflowEnvironment.start_local() as e:  # pyright: ignore[reportUnknownMemberType]
        yield e


@pytest.mark.integration
@pytest.mark.timeout(30)
@pytest.mark.usefixtures("_flush_valkey")
async def test_plugin_wires_interceptor(env: WorkflowEnvironment) -> None:
    """BackpressurePlugin creates the interceptor in _run_context; activity is gated."""
    config = TargetConfig(
        target_id=TARGET_ID,
        constraints=[Constraint(dimension="rps", limit=100)],
        tenant_weights={"t1": 1.0},
    )
    plugin = BackpressurePlugin(
        valkey_url="redis://localhost:6379",
        targets={TARGET_ID: config},
        activity_targets={ACTIVITY_NAME: TARGET_ID},
    )

    async with Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[_PluginWorkflow],
        activities=[_echo_activity],
        plugins=[plugin],
    ):
        result = await env.client.execute_workflow(
            _PluginWorkflow.run,
            "plugin",
            id="test-plugin",
            task_queue=TASK_QUEUE,
        )

    assert result == "echo:plugin"
