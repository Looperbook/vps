"""
Orchestrator package - Thin coordination layer.

This package contains the bot orchestrator that coordinates the main loop
and manages component lifecycle without business logic.
"""

from src.orchestrator.bot_orchestrator import (
    BotOrchestrator,
    OrchestratorConfig,
    CycleResult,
    OrchestratorFactory,
    CycleCallbacks,
    CycleAction,
    ComponentRefs,
)

__all__ = [
    "BotOrchestrator",
    "OrchestratorConfig",
    "CycleResult",
    "OrchestratorFactory",
    "CycleCallbacks",
    "CycleAction",
    "ComponentRefs",
]
