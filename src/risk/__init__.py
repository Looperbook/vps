"""
Risk management package.

This package contains the risk engine and related components for
drawdown protection, exposure limits, and PnL controls.
"""

from src.risk.risk import (
    RiskEngine,
    RiskEvent,
    RiskEventType,
    RiskSnapshot,
    RiskState,
)
from src.risk.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from src.risk.dead_man_switch import DeadManSwitch, DeadManSwitchStub

__all__ = [
    "RiskEngine",
    "RiskEvent",
    "RiskEventType",
    "RiskSnapshot",
    "RiskState",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "DeadManSwitch",
    "DeadManSwitchStub",
]
