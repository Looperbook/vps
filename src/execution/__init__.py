"""
Execution layer components for grid trading.

This package contains the extracted, single-responsibility modules
from the original bot.py "god file" architecture:

- FillProcessor: Handles fill processing, PnL calculation, state updates
- ExecutionGateway: Unified order placement and lifecycle management
- GridBuilder: Grid construction and placement orchestration
- RestPoller: REST fill polling for gap recovery
- ReconciliationService: Position and order reconciliation

Note: StateManager moved to src/state/, BotOrchestrator moved to src/orchestrator/
These are re-exported here for backward compatibility.
"""

from src.execution.fill_processor import FillProcessor, FillProcessorConfig, FillResult
from src.state.state_manager import StateManager, StateManagerConfig, StateSnapshot
from src.execution.execution_gateway import ExecutionGateway, ExecutionGatewayConfig, SubmitResult, CancelResult
from src.execution.grid_builder import GridBuilder, GridBuilderConfig, GridRebuildResult, ReplacementResult
from src.orchestrator.bot_orchestrator import (
    BotOrchestrator, OrchestratorConfig, CycleResult, OrchestratorFactory,
    CycleCallbacks, CycleAction, ComponentRefs,
)
from src.execution.rest_poller import RestPoller, RestPollerConfig, PollResult
from src.execution.reconciliation_service import (
    ReconciliationService, 
    ReconciliationConfig, 
    PositionReconcileResult, 
    OrderReconcileResult,
)

__all__ = [
    "FillProcessor", 
    "FillProcessorConfig", 
    "FillResult",
    "StateManager",
    "StateManagerConfig",
    "StateSnapshot",
    "ExecutionGateway",
    "ExecutionGatewayConfig",
    "SubmitResult",
    "CancelResult",
    "GridBuilder",
    "GridBuilderConfig",
    "GridRebuildResult",
    "ReplacementResult",
    "BotOrchestrator",
    "OrchestratorConfig",
    "CycleResult",
    "CycleCallbacks",
    "CycleAction",
    "ComponentRefs",
    "OrchestratorFactory",
    "RestPoller",
    "RestPollerConfig",
    "PollResult",
    "ReconciliationService",
    "ReconciliationConfig",
    "PositionReconcileResult",
    "OrderReconcileResult",
]
