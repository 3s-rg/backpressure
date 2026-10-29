"""Load Lua scripts and eval configuration from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import yaml
from loguru import logger

from .types import Constraint, TargetConfig

_LUA_DIR: Final[Path] = Path(__file__).resolve().parent / "lua"
_SCRIPT_CACHE: dict[str, str] = {}

_KNOWN_SCRIPTS: Final[dict[str, str]] = {
    "admission_token_buckets": "admission_token_buckets.lua",
    "admission_token_buckets_multi": "admission_token_buckets_multi.lua",
}


def get_lua_script(name: str) -> str:
    """Return the Lua script text for *name*, loading and caching on first call."""
    if name not in _KNOWN_SCRIPTS:
        msg = f"unknown script: {name!r}"
        raise ValueError(msg)
    if name not in _SCRIPT_CACHE:
        path = _LUA_DIR / _KNOWN_SCRIPTS[name]
        _SCRIPT_CACHE[name] = path.read_text(encoding="utf-8")
        logger.trace("loaded Lua script | name={name} path={path}", name=name, path=path)
    return _SCRIPT_CACHE[name]


# ---------------------------------------------------------------------------
# Eval config: backpressure.yaml
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UpstreamConfig:
    """Connection details for the upstream mock API."""

    url: str
    endpoint: str
    api_key_env: str = "MOCKLIMIT_API_KEY"


@dataclass(frozen=True)
class ActivityCostConfig:
    """How to compute resource costs for one activity."""

    target: str
    resource_costs: dict[str, str | int | float]
    """Map of dimension -> cost strategy or fixed value.

    Strategy strings: "characters_div_4", "fixed".
    Numeric values are treated as fixed costs.
    """


@dataclass
class BackpressureConfig:
    """Parsed backpressure.yaml config for the eval worker."""

    targets: dict[str, TargetConfig]
    upstreams: dict[str, UpstreamConfig]
    activities: dict[str, ActivityCostConfig]
    gate_mode: str = "inline"


def load_config(path: str | Path) -> BackpressureConfig:
    """Parse a backpressure.yaml config file into a typed config object.

    The YAML format:

        targets:
          <target_id>:
            constraints:
              - dimension: <str>
                limit: <float>        # rate per second
                default_cost: <float>  # optional
            upstream:
              url: <str>
              endpoint: <str>
              api_key_env: <str>      # env var name for API key

        activities:
          <activity_name>:
            target: <target_id>
            resource_costs:
              <dimension>: <int | float | strategy_name>

        tenant_weights:
          <tenant_id>: <float>

        gate_mode: inline | local-activity
    """
    config_path = Path(path)
    logger.info("Loading backpressure config from '{}'", config_path)
    raw: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    raw_weights: dict[str, Any] = dict(raw.get("tenant_weights") or {})
    tenant_weights: dict[str, float] = {str(k): float(v) for k, v in raw_weights.items()}

    targets: dict[str, TargetConfig] = {}
    upstreams: dict[str, UpstreamConfig] = {}

    raw_targets: dict[str, Any] = dict(raw.get("targets") or {})
    for target_id_raw, target_raw_val in raw_targets.items():
        target_id = str(target_id_raw)
        target_raw: dict[str, Any] = dict(target_raw_val)
        raw_constraints: list[dict[str, Any]] = list(target_raw.get("constraints", []))
        constraints = [
            Constraint(
                dimension=str(c["dimension"]),
                limit=float(c["limit"]),
                default_cost=float(c["default_cost"]) if "default_cost" in c else None,
            )
            for c in raw_constraints
        ]
        # Optional per-priority refill-share override. Absent -> {} -> the gate
        # falls back to its built-in defaults ({1:0.5, 2:0.3, 3:0.2}), existing
        # configs that omit the key are unaffected. Used by the ablation arms
        # that flatten priority shaping (equal shares).
        raw_priority: dict[str, Any] = dict(target_raw.get("priority_minimums") or {})
        priority_minimums: dict[int, float] = {int(k): float(v) for k, v in raw_priority.items()}
        targets[target_id] = TargetConfig(
            target_id=target_id,
            constraints=constraints,
            tenant_weights=tenant_weights,
            priority_minimums=priority_minimums,
        )

        upstream_raw: dict[str, Any] = dict(target_raw.get("upstream") or {})
        upstreams[target_id] = UpstreamConfig(
            url=str(upstream_raw.get("url", "http://localhost:8000")),
            endpoint=str(upstream_raw.get("endpoint", "/v1/chat/completions")),
            api_key_env=str(upstream_raw.get("api_key_env", "MOCKLIMIT_API_KEY")),
        )
        logger.debug(
            "Target '{}': {} constraint(s), upstream={}{}",
            target_id,
            len(constraints),
            upstream_raw.get("url"),
            upstream_raw.get("endpoint"),
        )

    activities: dict[str, ActivityCostConfig] = {}
    raw_activities: dict[str, Any] = dict(raw.get("activities") or {})
    for act_name_raw, act_raw_val in raw_activities.items():
        act_raw: dict[str, Any] = dict(act_raw_val)
        activities[str(act_name_raw)] = ActivityCostConfig(
            target=str(act_raw["target"]),
            resource_costs=dict(act_raw.get("resource_costs") or {}),
        )

    gate_mode = str(raw.get("gate_mode", "inline"))

    logger.info(
        "Backpressure config loaded: {} target(s), {} activity(s), mode={}",
        len(targets),
        len(activities),
        gate_mode,
    )
    return BackpressureConfig(
        targets=targets,
        upstreams=upstreams,
        activities=activities,
        gate_mode=gate_mode,
    )
