"""
Structured logging setup for grid bot.

Production-optimized:
- Async-safe queue handler to avoid blocking event loop
- Log level hierarchy for noise reduction
- Sampling support for high-frequency events
- Throttling for repetitive warnings
"""

from __future__ import annotations

import asyncio
import atexit
import json
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime
from typing import Dict, Optional, Set

try:  # rich is optional; fallback to plain stream if unavailable
    from rich.logging import RichHandler
except Exception:  # pragma: no cover - optional import
    RichHandler = None


# Log level constants for semantic clarity
CRITICAL_SAFETY = logging.CRITICAL  # Risk breaches, position drift, data loss
ERROR = logging.ERROR               # Failures requiring attention
WARNING = logging.WARNING           # Recoverable issues (WS reconnect, retries)
INFO = logging.INFO                 # Key lifecycle events (fills, reconcile)
DEBUG = logging.DEBUG               # High-frequency debug (order intents, mid prices)


class JsonFormatter(logging.Formatter):
    """Compact JSON formatter for structured log ingestion."""
    
    def format(self, record: logging.LogRecord) -> str:
        ts = time.time()
        payload = {
            "ts": ts,
            "ts_iso": datetime.fromtimestamp(ts).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))


class AsyncQueueHandler(logging.Handler):
    """
    Non-blocking handler that queues log records for background processing.
    
    Prevents logging from blocking the asyncio event loop. Records are
    written by a dedicated background thread.
    """
    
    def __init__(self, target_handler: logging.Handler, max_queue_size: int = 10000):
        super().__init__()
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._target = target_handler
        self._shutdown = False
        self._thread = threading.Thread(target=self._worker, daemon=True, name="log-writer")
        self._thread.start()
        self._dropped = 0
        atexit.register(self.close)
    
    def emit(self, record: logging.LogRecord) -> None:
        if self._shutdown:
            return
        try:
            # Non-blocking put; drop if queue full
            self._queue.put_nowait(record)
        except queue.Full:
            self._dropped += 1
    
    def _worker(self) -> None:
        while not self._shutdown or not self._queue.empty():
            try:
                record = self._queue.get(timeout=0.1)
                self._target.emit(record)
                self._queue.task_done()
            except queue.Empty:
                continue
            except Exception:
                pass
    
    def close(self) -> None:
        self._shutdown = True
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)
        if self._dropped > 0:
            sys.stderr.write(f"[logging] Dropped {self._dropped} log records due to queue overflow\n")
        self._target.close()
        super().close()


class ThrottledFilter(logging.Filter):
    """
    Filter that throttles repetitive log messages.
    
    Allows first occurrence, then suppresses duplicates for cooldown_sec.
    Useful for warnings that can fire rapidly (WS stale, retries).
    """
    
    def __init__(self, cooldown_sec: float = 30.0, throttled_events: Optional[Set[str]] = None):
        super().__init__()
        self._cooldown = cooldown_sec
        self._last_seen: Dict[str, float] = {}
        self._throttled_events = throttled_events or {
            "ws_stale_detected", "http_retry", "ws_stale_halt",
        }
    
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        # Try to extract event name from JSON
        try:
            data = json.loads(msg)
            event = data.get("event", "")
        except (json.JSONDecodeError, TypeError):
            return True  # Not JSON, allow through
        
        if event not in self._throttled_events:
            return True
        
        now = time.time()
        key = f"{event}:{data.get('coin', '')}"
        last = self._last_seen.get(key, 0)
        
        if now - last < self._cooldown:
            return False  # Suppress
        
        self._last_seen[key] = now
        return True


def build_logger(
    name: str = "gridbot",
    level: int = logging.INFO,
    file_path: Optional[str] = "gridbot.log",
    async_file: bool = True,
    throttle_warnings: bool = True,
) -> logging.Logger:
    """
    Build production-optimized logger.
    
    Args:
        name: Logger name
        level: Minimum log level
        file_path: Path to log file (None to disable file logging)
        async_file: Use async queue handler for file to avoid blocking
        throttle_warnings: Apply throttling filter to reduce repetitive warnings
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Idempotent handler setup
    if logger.handlers:
        for h in logger.handlers:
            h.setLevel(level)
        return logger
    
    # Console: human-friendly if rich available, otherwise JSON
    if RichHandler:
        stream_handler = RichHandler(
            rich_tracebacks=False,
            show_time=True,
            show_level=True,
            show_path=False,
            markup=True,
        )
        stream_handler.setLevel(level)
        stream_handler.setFormatter(logging.Formatter("%(message)s"))
    else:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(JsonFormatter())
        stream_handler.setLevel(level)
    
    # Apply throttle filter to reduce console spam
    if throttle_warnings:
        stream_handler.addFilter(ThrottledFilter(cooldown_sec=30.0))
    
    logger.addHandler(stream_handler)
    
    # File: structured JSON for downstream ingestion
    if file_path:
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(JsonFormatter())
        file_handler.setLevel(level)
        
        if async_file:
            # Wrap in async handler to avoid blocking event loop
            async_handler = AsyncQueueHandler(file_handler, max_queue_size=10000)
            async_handler.setLevel(level)
            logger.addHandler(async_handler)
        else:
            logger.addHandler(file_handler)
    
    logger.propagate = False
    return logger


# Convenience function for structured event logging
def log_event(
    logger: logging.Logger,
    event: str,
    level: int = logging.INFO,
    **data
) -> None:
    """
    Log a structured event with proper level.
    
    Usage:
        log_event(log, "fill", level=INFO, side="buy", px=100.0)
    """
    payload = {"event": event, **data}
    logger.log(level, json.dumps(payload))

