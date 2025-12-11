"""
BotLogger: Centralized logging for GridBot with level management and sampling.

This module extracts the logging logic from bot.py to provide:
- Structured JSON logging
- Log level hierarchy
- Sampling for high-frequency events
- Event categorization (critical, error, warning, info, debug)

Usage:
    logger = BotLogger(coin="BTC")
    logger.log("fill", side="buy", px=100.0, sz=1.0)
    logger.log("grid_rebuild_complete", mid=100.0, placed=10)
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Set

log = logging.getLogger("gridbot")


@dataclass 
class BotLoggerConfig:
    """Configuration for BotLogger."""
    # Sampling rates for high-frequency events (0.0-1.0)
    order_intent_sample_rate: float = 0.0  # Default: don't sample intents
    strategy_sample_rate: float = 0.0
    submit_sample_rate: float = 0.05
    
    # Throttle window for repetitive warnings
    throttle_window_sec: float = 60.0
    
    # Enable debug logging
    debug_enabled: bool = False


class BotLogger:
    """
    Centralized logging for GridBot with level management.
    
    Log Level Hierarchy:
    - CRITICAL: Risk breaches, position drift, data loss (always logged)
    - ERROR: Failures requiring attention (always logged)
    - WARNING: Recoverable issues (throttled for repetitive events)
    - INFO: Key lifecycle events (fills, reconcile, grid builds)
    - DEBUG: High-frequency events (order intents, mid prices, acks)
    """
    
    # Event -> log level categorization
    CRITICAL_EVENTS: Set[str] = {
        "position_drift_detected", "fill_log_write_error", "unusual_pnl_fill",
        "risk_halt", "risk_breach", "data_halt", "wal_write_error",
        "state_integrity_error", "shadow_ledger_drift",
    }
    
    ERROR_EVENTS: Set[str] = {
        "order_submit_error", "cancel_error", "http_error", "ws_error",
        "reconcile_error", "fill_error", "batch_majority_failed",
        "grid_rebuild_aborted_drift",
    }
    
    WARNING_EVENTS: Set[str] = {
        "order_ack_missing_oid", "fill_dedup_skip", "skip_replacement_risk",
        "flatten_skip_refill", "grid_position_drift", "circuit_breaker_open",
        "rest_poll_degraded", "batch_submit_errors", "fill_timestamp_anomaly",
    }
    
    DEBUG_EVENTS: Set[str] = {
        "order_intent", "mid_price", "order_submit_ack", "ws_message",
        "rest_fills_polled", "heartbeat", "cycle_start", "cycle_end",
    }
    
    # Events to throttle (only log once per window)
    THROTTLE_EVENTS: Set[str] = {
        "fill_dedup_skip", "order_ack_missing_oid", "skip_replacement_risk",
    }
    
    def __init__(
        self,
        coin: str,
        config: Optional[BotLoggerConfig] = None,
    ) -> None:
        """
        Initialize BotLogger.
        
        Args:
            coin: Trading symbol for log context
            config: Optional configuration
        """
        self.coin = coin
        self.config = config or BotLoggerConfig()
        
        # Throttle tracking: event -> last_logged_time
        self._throttle_times: Dict[str, float] = {}
        
        # Sampling counter for consistent sampling
        self._sample_counter: int = 0
    
    def log(self, event: str, **data: Any) -> None:
        """
        Log an event with proper level and formatting.
        
        Args:
            event: Event name
            **data: Event data
        """
        # Determine log level
        if event in self.CRITICAL_EVENTS:
            level = logging.CRITICAL
        elif event in self.ERROR_EVENTS:
            level = logging.ERROR
        elif event in self.WARNING_EVENTS:
            level = logging.WARNING
        elif event in self.DEBUG_EVENTS:
            level = logging.DEBUG
        else:
            level = logging.INFO
        
        # Check throttling for certain events
        if event in self.THROTTLE_EVENTS:
            now = time.time()
            last_time = self._throttle_times.get(event, 0.0)
            if now - last_time < self.config.throttle_window_sec:
                return  # Skip throttled event
            self._throttle_times[event] = now
        
        # Check sampling for high-frequency events
        if event == "order_intent":
            if not self._should_sample(self.config.order_intent_sample_rate):
                return
        elif event == "order_submit_ack":
            if not self._should_sample(self.config.submit_sample_rate):
                return
        
        # Skip debug events if not enabled
        if level == logging.DEBUG and not self.config.debug_enabled:
            return
        
        # Build log payload
        payload = {"event": event, "coin": self.coin, **data}
        
        # Log at appropriate level
        log.log(level, json.dumps(payload))
    
    def _should_sample(self, rate: float) -> bool:
        """
        Determine if event should be sampled based on rate.
        
        Args:
            rate: Sampling rate (0.0-1.0)
            
        Returns:
            True if event should be logged
        """
        if rate <= 0.0:
            return False
        if rate >= 1.0:
            return True
        
        self._sample_counter += 1
        # Use modulo for consistent sampling
        threshold = int(1.0 / rate)
        return (self._sample_counter % threshold) == 0
    
    def critical(self, event: str, **data: Any) -> None:
        """Log critical event (always logged)."""
        payload = {"event": event, "coin": self.coin, **data}
        log.critical(json.dumps(payload))
    
    def error(self, event: str, **data: Any) -> None:
        """Log error event (always logged)."""
        payload = {"event": event, "coin": self.coin, **data}
        log.error(json.dumps(payload))
    
    def warning(self, event: str, **data: Any) -> None:
        """Log warning event."""
        payload = {"event": event, "coin": self.coin, **data}
        log.warning(json.dumps(payload))
    
    def info(self, event: str, **data: Any) -> None:
        """Log info event."""
        payload = {"event": event, "coin": self.coin, **data}
        log.info(json.dumps(payload))
    
    def debug(self, event: str, **data: Any) -> None:
        """Log debug event (if enabled)."""
        if self.config.debug_enabled:
            payload = {"event": event, "coin": self.coin, **data}
            log.debug(json.dumps(payload))
    
    def get_callback(self) -> Callable[..., None]:
        """
        Get a callback function for use with other components.
        
        Returns:
            Callable that logs events
        """
        return self.log
