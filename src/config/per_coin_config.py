"""Load per-coin configuration overrides from YAML.

Optional file path via env `HL_PER_COIN_CONFIG`, default `configs/per_coin.yaml`.
Returns a dict mapping coin -> dict of overrides.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Any

try:
    import yaml
except Exception:  # pragma: no cover - yaml optional
    yaml = None  # type: ignore


def load_per_coin_overrides(path: str | None = None) -> Dict[str, Dict[str, Any]]:
    if path is None:
        path = os.getenv("HL_PER_COIN_CONFIG", "configs/per_coin.yaml")
    p = Path(path)
    if not p.exists() or yaml is None:
        return {}
    try:
        with p.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
            if isinstance(data, dict):
                return {k: v for k, v in data.items() if isinstance(v, dict)}
    except Exception:
        return {}
    return {}
