"""
CircuitBreaker: Centralized circuit breaker for API error handling.

Extracted from bot.py to improve maintainability and testability.
Handles:
- Error streak tracking
- Circuit trip/reset logic
- Cooldown management
- Configurable thresholds
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

log = logging.getLogger("gridbot")


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    error_threshold: int = 5  # Number of consecutive errors to trip
    cooldown_sec: float = 10.0  # Seconds to wait before resetting
    backoff_multiplier: float = 2.0  # Multiplier for cooldown on repeated trips


class CircuitBreaker:
    """
    Circuit breaker pattern implementation for API error handling.
    
    When errors exceed threshold, trips the circuit and enters cooldown.
    During cooldown, is_tripped returns True to prevent further API calls.
    After cooldown expires, resets automatically.
    
    Thread-safe for single-threaded asyncio usage (no internal locks).
    """
    
    def __init__(self, config: CircuitBreakerConfig, 
                 on_trip: Optional[Callable[[], None]] = None,
                 on_reset: Optional[Callable[[], None]] = None,
                 log_event: Optional[Callable[..., None]] = None) -> None:
        self.config = config
        self.error_streak: int = 0
        self._tripped: bool = False
        self._cooldown_until: float = 0.0
        self._trip_count: int = 0
        
        # Callbacks for integration with bot lifecycle
        self._on_trip = on_trip
        self._on_reset = on_reset
        self._log_event = log_event or self._default_log
    
    def _default_log(self, event: str, **kwargs: Any) -> None:
        """Default logging implementation."""
        log.info(json.dumps({"event": event, **kwargs}))
    
    @property
    def is_tripped(self) -> bool:
        """Check if circuit is currently tripped. Auto-resets after cooldown."""
        if self._tripped and time.time() >= self._cooldown_until:
            self._reset()
            return False
        return self._tripped
    
    @property
    def cooldown_remaining(self) -> float:
        """Seconds remaining in cooldown, or 0 if not tripped."""
        if not self._tripped:
            return 0.0
        return max(0.0, self._cooldown_until - time.time())
    
    def record_error(self, where: str, error: Exception) -> bool:
        """
        Record an API error. Returns True if circuit tripped as a result.
        
        Args:
            where: Label for where the error occurred (for logging)
            error: The exception that occurred
            
        Returns:
            True if this error caused the circuit to trip
        """
        self.error_streak += 1
        self._log_event("api_error", where=where, err=str(error), streak=self.error_streak)
        
        if self.error_streak >= self.config.error_threshold:
            return self._trip(where)
        return False
    
    def record_success(self) -> None:
        """Record a successful API call. Resets error streak."""
        if self.error_streak > 0:
            self._log_event("api_error_reset", streak=self.error_streak)
            self.error_streak = 0
    
    def _trip(self, where: str) -> bool:
        """Trip the circuit breaker."""
        if self._tripped:
            return False  # Already tripped
        
        self._tripped = True
        self._trip_count += 1
        
        # Calculate cooldown with exponential backoff for repeated trips
        base_cooldown = max(5.0, self.config.cooldown_sec)
        backoff = min(self.config.backoff_multiplier ** min(self._trip_count - 1, 5), 32.0)
        cooldown = base_cooldown * backoff
        self._cooldown_until = time.time() + cooldown
        
        self._log_event("circuit_break", 
                       where=where, 
                       streak=self.error_streak,
                       trip_count=self._trip_count,
                       cooldown_sec=cooldown)
        
        if self._on_trip:
            try:
                self._on_trip()
            except Exception:
                pass
        
        return True
    
    def _reset(self) -> None:
        """Reset the circuit breaker after cooldown."""
        was_tripped = self._tripped
        self._tripped = False
        self.error_streak = 0
        
        if was_tripped:
            self._log_event("circuit_reset", trip_count=self._trip_count)
            
            if self._on_reset:
                try:
                    self._on_reset()
                except Exception:
                    pass
    
    def force_reset(self) -> None:
        """Force immediate reset of circuit breaker."""
        self._cooldown_until = 0.0
        self._reset()
    
    def force_trip(self, cooldown_sec: Optional[float] = None) -> None:
        """Force trip the circuit breaker (e.g., for manual intervention)."""
        self._tripped = True
        self._trip_count += 1
        cooldown = cooldown_sec if cooldown_sec is not None else self.config.cooldown_sec
        self._cooldown_until = time.time() + cooldown
        self._log_event("circuit_force_trip", cooldown_sec=cooldown)
    
    def get_state(self) -> dict:
        """Get current state for persistence/monitoring."""
        return {
            "tripped": self._tripped,
            "error_streak": self.error_streak,
            "trip_count": self._trip_count,
            "cooldown_until": self._cooldown_until,
            "cooldown_remaining": self.cooldown_remaining,
        }
