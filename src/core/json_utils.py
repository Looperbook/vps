"""
Fast JSON utilities for high-frequency operations.

Uses orjson when available (3-10x faster than stdlib json).
Falls back to stdlib json for compatibility.

Usage:
    from src.core.json_utils import dumps, loads
    
    # Fast serialization for logging
    log.info(dumps({"event": "fill", "px": 100.0}))
"""

from __future__ import annotations

from typing import Any

try:
    import orjson
    
    def dumps(obj: Any) -> str:
        """Fast JSON encode to string."""
        return orjson.dumps(obj).decode("utf-8")
    
    def dumps_bytes(obj: Any) -> bytes:
        """Fast JSON encode to bytes (even faster, skip utf-8 decode)."""
        return orjson.dumps(obj)
    
    def loads(s: str | bytes) -> Any:
        """Fast JSON decode."""
        return orjson.loads(s)
    
    ORJSON_AVAILABLE = True

except ImportError:
    import json
    
    def dumps(obj: Any) -> str:
        """Stdlib JSON encode with compact separators."""
        return json.dumps(obj, separators=(",", ":"))
    
    def dumps_bytes(obj: Any) -> bytes:
        """Stdlib JSON encode to bytes."""
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")
    
    def loads(s: str | bytes) -> Any:
        """Stdlib JSON decode."""
        return json.loads(s)
    
    ORJSON_AVAILABLE = False
