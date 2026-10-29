"""Resource cost estimation for admission requests.

Translates the ``resource_costs`` section of backpressure.yaml into actual
numeric costs at request time, based on the request body.
"""

from __future__ import annotations

import json
from typing import Any

__all__ = ["estimate_costs"]


def estimate_costs(
    strategies: dict[str, str | int | float],
    body: dict[str, Any] | None = None,
) -> dict[str, float]:
    """Compute per-dimension costs for one request.

    ``strategies`` maps dimension names to either a fixed numeric value
    or a strategy name string:

    - ``"characters_div_4"``: ``len(json_body) // 4``
    - ``"fixed"`` or any numeric: use as-is
    """
    body_text: str | None = None
    costs: dict[str, float] = {}

    for dimension, strategy in strategies.items():
        if isinstance(strategy, (int, float)):
            costs[dimension] = float(strategy)
        elif strategy == "characters_div_4":
            if body_text is None:
                body_text = json.dumps(body) if body else ""
            costs[dimension] = float(len(body_text) // 4)
        else:
            costs[dimension] = 1.0

    return costs
