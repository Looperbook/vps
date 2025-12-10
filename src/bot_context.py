"""
Structured logging context with trace IDs for production observability.

Each logical operation (fill, order, reconciliation, etc.) gets a unique trace_id
so all related log entries can be correlated across microservices or workers.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Optional


class BotContext:
    """Request context for structured logging with trace ID correlation."""

    def __init__(
        self,
        coin: str,
        trace_id: Optional[str] = None,
        parent_trace_id: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize context.

        Args:
            coin: trading pair (e.g., 'NVDA', 'BTC')
            trace_id: unique ID for this logical operation (auto-generated if None)
            parent_trace_id: parent trace ID for nested operations
            logger: logger instance (auto-created if None)
        """
        self.coin = coin
        self.trace_id = trace_id or str(uuid.uuid4())
        self.parent_trace_id = parent_trace_id
        self.start_time = time.time()
        self.logger = logger or logging.getLogger("gridbot")
        self.tags: dict[str, Any] = {}

    def set_tag(self, key: str, value: Any) -> None:
        """Add a tag to be included in all logs from this context."""
        self.tags[key] = value

    def log(
        self,
        event: str,
        level: str = "info",
        **data: Any
    ) -> None:
        """
        Log a structured event with trace ID and elapsed time.

        Args:
            event: event name (e.g., 'fill_received', 'order_submitted')
            level: log level ('debug', 'info', 'warning', 'error')
            **data: additional fields to log
        """
        elapsed_ms = (time.time() - self.start_time) * 1000.0
        payload = {
            "ts": time.time(),
            "trace_id": self.trace_id,
            **({"parent_trace_id": self.parent_trace_id} if self.parent_trace_id else {}),
            "coin": self.coin,
            "event": event,
            "elapsed_ms": elapsed_ms,
            **self.tags,
            **data
        }
        log_func = getattr(self.logger, level, self.logger.info)
        log_func(json.dumps(payload))

    def debug(self, event: str, **data: Any) -> None:
        """Log at DEBUG level."""
        self.log(event, level="debug", **data)

    def info(self, event: str, **data: Any) -> None:
        """Log at INFO level."""
        self.log(event, level="info", **data)

    def warning(self, event: str, **data: Any) -> None:
        """Log at WARNING level."""
        self.log(event, level="warning", **data)

    def error(self, event: str, **data: Any) -> None:
        """Log at ERROR level."""
        self.log(event, level="error", **data)

    def child(self, sub_operation: str) -> BotContext:
        """
        Create a child context for a sub-operation.

        The child will have a new trace_id and parent_trace_id set to this context's trace_id.
        """
        child = BotContext(
            coin=self.coin,
            trace_id=str(uuid.uuid4()),
            parent_trace_id=self.trace_id,
            logger=self.logger
        )
        child.set_tag("sub_operation", sub_operation)
        return child

    def elapsed_ms(self) -> float:
        """Get elapsed time since context creation in milliseconds."""
        return (time.time() - self.start_time) * 1000.0
