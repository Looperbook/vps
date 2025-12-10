"""
State persistence helpers.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from src.logging_cfg import build_logger

log = build_logger("state")


class StateStore:
    def __init__(self, coin: str, state_dir: str) -> None:
        safe = coin.replace(":", "_").replace("/", "_")
        self.path = Path(state_dir) / f"bot_state_{safe}.json"
        self.tmp = self.path.with_suffix(".tmp")
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except Exception as exc:
            log.error(f"state_load_error:{exc}")
            return {}

    def save(self, data: Dict[str, Any]) -> None:
        try:
            self.tmp.write_text(json.dumps(data, indent=2))
            self.tmp.replace(self.path)
        except Exception as exc:
            log.error(f"state_save_error:{exc}")
