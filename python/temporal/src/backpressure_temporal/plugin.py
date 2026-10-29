"""Backpressure Temporal plugin, transparent rate limiting for activities."""

from __future__ import annotations

import dataclasses
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from temporalio.plugin import SimplePlugin
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from backpressure.types import TargetConfig
from backpressure_temporal._local_activities import bp_acquire, bp_inline_run, bp_release

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from temporalio.worker import WorkflowRunner

__all__ = ["BackpressurePlugin"]

_PASSTHROUGH_MODULES = ("backpressure", "backpressure_temporal")


class BackpressurePlugin(SimplePlugin):
    """Drop-in Temporal plugin that adds Backpressure rate limiting.

    Admission (``queued_acquire`` / ``queued_release``) runs as a pair of
    internal Temporal local activities (``__bp_acquire`` / ``__bp_release``)
    registered automatically on every worker that loads this plugin.  The
    regular activity slot for gated activities is held only for the actual
    user work, not during the Valkey queue poll, so the Temporal matching
    service is no longer in the admission path.

    When the worker is started with ``BACKPRESSURE_GATE_MODE=inline`` and the
    plugin is configured with ``inline_activities=``, ``gated_activity()``
    instead schedules a single regular activity (``__bp_inline_run``) that
    folds the gate acquire and the user activity body into one dispatch.
    Trades activity-slot-time for fewer workflow tasks; useful when matching
    dispatch round-trips dominate end-to-end latency.

    Usage::

        plugin = BackpressurePlugin(
            valkey_url="redis://localhost:6379",
            targets={"llm-api": TargetConfig(...)},
            activity_targets={"call_llm": "llm-api"},
            inline_activities={"call_llm": call_llm},  # required for inline mode
        )
        worker = Worker(client, ..., plugins=[plugin])

    In workflow code use :func:`backpressure_temporal.gated_activity` as the
    single entry point for gated execution.
    """

    def __init__(
        self,
        *,
        valkey_url: str = "redis://localhost:6379",
        targets: dict[str, TargetConfig] | None = None,
        activity_targets: dict[str, str] | None = None,
        inline_activities: dict[str, Callable[..., Any]] | None = None,
    ) -> None:
        """Create a Backpressure plugin.

        Parameters
        ----------
        valkey_url:
            Connection string for the Valkey instance.
        targets:
            Mapping of target ID to ``TargetConfig``.
        activity_targets:
            Mapping of Temporal activity type name to target ID.  Used by
            :func:`~backpressure_temporal.gated_activity` to resolve the
            target when ``target_id`` is not passed explicitly.
        inline_activities:
            Mapping of Temporal activity type name to the user function the
            inline-mode wrapper should invoke in-process.  Required for
            ``BACKPRESSURE_GATE_MODE=inline``; ignored otherwise.  The same
            function should also be in the worker's ``activities=`` list so
            that local-activity mode keeps working.

        """
        self._valkey_url = valkey_url
        self.targets: dict[str, TargetConfig] = targets or {}
        self.activity_targets: dict[str, str] = activity_targets or {}
        self.inline_activities: dict[str, Callable[..., Any]] = inline_activities or {}

        super().__init__(  # pyright: ignore[reportUnknownMemberType]
            "backpressure.RateLimiter",
            activities=[bp_acquire, bp_release, bp_inline_run],
            workflow_runner=self.patch_workflow_runner,
            run_context=self._run_context,
        )

    @classmethod
    def from_config(
        cls,
        path: str | Path,
        *,
        valkey_url: str = "redis://localhost:6379",
    ) -> BackpressurePlugin:
        """Load targets and activity_targets from a YAML or JSON file.

        YAML requires ``pyyaml`` to be installed.
        """
        filepath = Path(path)
        raw_text = filepath.read_text()

        if filepath.suffix in {".yaml", ".yml"}:
            import yaml  # noqa: PLC0415  # pyright: ignore[reportMissingModuleSource]

            raw: dict[str, Any] = yaml.safe_load(raw_text)
        else:
            raw = json.loads(raw_text)

        raw_targets = cast("dict[str, dict[str, object]]", raw.get("targets", {}))
        targets: dict[str, TargetConfig] = {}
        for target_id, target_dict in raw_targets.items():
            d = dict(target_dict)
            d.setdefault("target_id", target_id)
            targets[target_id] = TargetConfig.from_dict(d)

        activity_targets = cast(
            "dict[str, str]",
            raw.get("activity_targets", {}),
        )

        return cls(
            valkey_url=valkey_url,
            targets=targets,
            activity_targets=activity_targets,
        )

    @asynccontextmanager
    async def _run_context(self) -> AsyncIterator[None]:
        """Manage the Valkey connection pool and populate module-level state."""
        # Late imports keep workflow-sandbox passthrough clean: pulling Valkey
        # at module load would import the network stack into every sandbox.
        from valkey.asyncio import Valkey as AsyncValkey  # noqa: PLC0415

        from backpressure_temporal import _local_activities as _la  # noqa: PLC0415

        pool = AsyncValkey.from_url(self._valkey_url, decode_responses=False)
        _la.valkey_pool = pool
        _la.targets = self.targets
        _la.activity_target_map = dict(self.activity_targets)
        _la.inline_activity_fns = dict(self.inline_activities)

        try:
            yield
        finally:
            _la.valkey_pool = None
            _la.targets = {}
            _la.activity_target_map = {}
            _la.inline_activity_fns = {}
            await pool.aclose()

    @staticmethod
    def patch_workflow_runner(runner: WorkflowRunner | None) -> WorkflowRunner:
        """Add ``backpressure`` / ``backpressure_temporal`` to sandbox passthroughs."""
        if isinstance(runner, SandboxedWorkflowRunner):
            return dataclasses.replace(
                runner,
                restrictions=runner.restrictions.with_passthrough_modules(
                    *_PASSTHROUGH_MODULES,
                ),
            )
        if runner is None:
            return SandboxedWorkflowRunner(
                restrictions=SandboxedWorkflowRunner().restrictions.with_passthrough_modules(
                    *_PASSTHROUGH_MODULES,
                ),
            )
        return runner
