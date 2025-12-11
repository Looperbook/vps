"""
Tests for BotOrchestrator - coordination layer for grid bot components.

Tests cover:
- Fill processing delegation
- Cycle execution
- State management integration
- Graceful shutdown
- Factory methods
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

from src.orchestrator.bot_orchestrator import (
    BotOrchestrator,
    OrchestratorConfig,
    CycleResult,
    OrchestratorFactory,
)


@dataclass
class MockFillResult:
    """Mock fill result."""
    success: bool = True
    pnl: float = 0.0
    fill_id: str = "test-fill"


class MockFillProcessor:
    """Mock fill processor."""
    def __init__(self):
        self.fills_processed: List[Dict] = []
        self.process_result = MockFillResult()
    
    async def process_fill(
        self,
        fill: Dict[str, Any],
        position: float,
        pre_fee_cost: Optional[float] = None,
        **kwargs,
    ) -> MockFillResult:
        self.fills_processed.append(fill)
        return self.process_result


@dataclass
class MockStateSnapshot:
    """Mock state snapshot."""
    position: float = 0.0
    session_realized_pnl: float = 0.0
    alltime_realized_pnl: float = 0.0


class MockStateManager:
    """Mock state manager."""
    def __init__(self):
        self.save_called = False
        self._snapshot = MockStateSnapshot()
    
    def snapshot(self) -> MockStateSnapshot:
        return self._snapshot
    
    async def persist(self) -> None:
        """Persist state (renamed from save_state)."""
        self.save_called = True
    
    async def save_state(self) -> None:
        """Alias for persist for backward compatibility in tests."""
        await self.persist()


class MockExecutionGateway:
    """Mock execution gateway."""
    def __init__(self):
        self.cancel_all_called = False
        self.cancel_reason: Optional[str] = None
    
    async def cancel_all(self, reason: str = "") -> None:
        self.cancel_all_called = True
        self.cancel_reason = reason


@dataclass
class MockGridRebuildResult:
    """Mock grid rebuild result."""
    success: bool = True
    mid: float = 100.0


class MockGridBuilder:
    """Mock grid builder."""
    def __init__(self):
        self._rebuild_needed = False
        self.rebuild_called = False
        self.rebuild_result = MockGridRebuildResult()
        self.clear_called = False
    
    def should_rebuild(self) -> bool:
        return self._rebuild_needed
    
    def clear_rebuild_flag(self) -> None:
        self.clear_called = True
        self._rebuild_needed = False
    
    async def build_and_place_grid(
        self,
        mid: float,
        position: float,
        position_lock: asyncio.Lock,
        get_current_position,
    ) -> MockGridRebuildResult:
        self.rebuild_called = True
        return self.rebuild_result


@pytest.fixture
def mock_components():
    """Create mock components."""
    return {
        "fill_processor": MockFillProcessor(),
        "state_manager": MockStateManager(),
        "execution_gateway": MockExecutionGateway(),
        "grid_builder": MockGridBuilder(),
    }


@pytest.fixture
def orchestrator(mock_components):
    """Create orchestrator with mocks."""
    config = OrchestratorConfig(
        rebuild_interval_sec=1.0,
        state_save_interval_sec=1.0,
        pnl_log_interval_sec=1.0,
        use_state_manager=True,
        use_execution_gateway=True,
        use_grid_builder=True,
    )
    return BotOrchestrator(
        coin="TEST",
        fill_processor=mock_components["fill_processor"],
        state_manager=mock_components["state_manager"],
        execution_gateway=mock_components["execution_gateway"],
        grid_builder=mock_components["grid_builder"],
        config=config,
    ), mock_components


class TestOrchestratorBasics:
    """Basic orchestrator tests."""
    
    def test_create_orchestrator(self, orchestrator):
        """Test orchestrator creation."""
        orch, components = orchestrator
        assert orch.coin == "TEST"
        assert orch.fill_processor is not None
        assert orch.is_running is True
    
    def test_stop(self, orchestrator):
        """Test orchestrator stop."""
        orch, _ = orchestrator
        assert orch.is_running is True
        orch.stop()
        assert orch.is_running is False
    
    def test_get_stats(self, orchestrator):
        """Test stats retrieval."""
        orch, _ = orchestrator
        stats = orch.get_stats()
        assert stats["coin"] == "TEST"
        assert stats["running"] is True
        assert stats["cycle_count"] == 0
        assert stats["components"]["fill_processor"] is True


class TestFillProcessing:
    """Fill processing tests."""
    
    @pytest.mark.asyncio
    async def test_process_fill_delegates(self, orchestrator):
        """Test fill processing delegates to FillProcessor."""
        orch, components = orchestrator
        
        fill = {"tid": 123, "px": "100.0", "sz": "1.0", "side": "B"}
        result = await orch.process_fill(fill=fill, position=0.0)
        
        assert result.success is True
        assert len(components["fill_processor"].fills_processed) == 1
        assert components["fill_processor"].fills_processed[0] == fill


class TestCycleExecution:
    """Cycle execution tests."""
    
    @pytest.mark.asyncio
    async def test_cycle_basic(self, orchestrator):
        """Test basic cycle execution."""
        orch, _ = orchestrator
        
        result = await orch.cycle(
            mid=100.0,
            position=0.0,
        )
        
        assert result.success is True
        assert result.mid == 100.0
        assert orch._cycle_count == 1
    
    @pytest.mark.asyncio
    async def test_cycle_with_fill_queue(self, orchestrator):
        """Test cycle processes fill queue."""
        orch, components = orchestrator
        
        fill_queue = asyncio.Queue()
        await fill_queue.put({"tid": 1, "px": "100.0", "sz": "1.0", "side": "B"})
        await fill_queue.put({"tid": 2, "px": "101.0", "sz": "1.0", "side": "A"})
        
        result = await orch.cycle(
            mid=100.0,
            position=0.0,
            fill_queue=fill_queue,
        )
        
        assert result.success is True
        assert result.fills_processed == 2
        assert len(components["fill_processor"].fills_processed) == 2
    
    @pytest.mark.asyncio
    async def test_cycle_triggers_rebuild_when_needed(self, orchestrator):
        """Test cycle triggers rebuild when GridBuilder signals need."""
        orch, components = orchestrator
        position_lock = asyncio.Lock()
        
        # Signal rebuild needed
        components["grid_builder"]._rebuild_needed = True
        
        result = await orch.cycle(
            mid=100.0,
            position=0.0,
            position_lock=position_lock,
            get_current_position=lambda: 0.0,
        )
        
        assert result.success is True
        assert result.grid_rebuilt is True
        assert components["grid_builder"].rebuild_called is True
        assert components["grid_builder"].clear_called is True
    
    @pytest.mark.asyncio
    async def test_cycle_no_rebuild_without_lock(self, orchestrator):
        """Test cycle doesn't rebuild without position lock."""
        orch, components = orchestrator
        
        # Signal rebuild needed but don't provide lock
        components["grid_builder"]._rebuild_needed = True
        
        result = await orch.cycle(
            mid=100.0,
            position=0.0,
            # No position_lock provided
        )
        
        assert result.success is True
        assert result.grid_rebuilt is False  # Can't rebuild without lock
    
    @pytest.mark.asyncio
    async def test_cycle_handles_error(self, orchestrator):
        """Test cycle handles errors gracefully in fill queue processing."""
        orch, components = orchestrator
        
        # Make fill processor raise
        errors_logged = []
        orch._log_event = lambda e, **k: errors_logged.append((e, k))
        
        async def raise_error(*args, **kwargs):
            raise ValueError("Test error")
        components["fill_processor"].process_fill = raise_error
        
        fill_queue = asyncio.Queue()
        await fill_queue.put({"tid": 1})
        
        result = await orch.cycle(
            mid=100.0,
            position=0.0,
            fill_queue=fill_queue,
        )
        
        # Cycle still succeeds even if fill processing has errors
        # (errors are logged but don't fail the entire cycle)
        assert result.success is True
        # Check that the error was logged
        error_events = [e for e, k in errors_logged if e == "fill_queue_process_error"]
        assert len(error_events) == 1


class TestShutdown:
    """Shutdown tests."""
    
    @pytest.mark.asyncio
    async def test_shutdown_cancels_orders(self, orchestrator):
        """Test shutdown cancels all orders."""
        orch, components = orchestrator
        
        await orch.shutdown()
        
        assert orch.is_running is False
        assert components["execution_gateway"].cancel_all_called is True
        assert components["execution_gateway"].cancel_reason == "shutdown"
    
    @pytest.mark.asyncio
    async def test_shutdown_saves_state(self, orchestrator):
        """Test shutdown saves final state."""
        orch, components = orchestrator
        
        await orch.shutdown()
        
        assert components["state_manager"].save_called is True
    
    @pytest.mark.asyncio
    async def test_shutdown_handles_cancel_error(self, mock_components):
        """Test shutdown handles cancel errors."""
        # Make execution gateway raise
        async def raise_error(*args, **kwargs):
            raise ValueError("Cancel failed")
        mock_components["execution_gateway"].cancel_all = raise_error
        
        config = OrchestratorConfig(use_execution_gateway=True)
        orch = BotOrchestrator(
            coin="TEST",
            fill_processor=mock_components["fill_processor"],
            execution_gateway=mock_components["execution_gateway"],
            config=config,
        )
        
        # Should not raise
        await orch.shutdown()
        assert orch.is_running is False


class TestMinimalOrchestrator:
    """Tests for minimal (FillProcessor only) orchestrator."""
    
    @pytest.mark.asyncio
    async def test_minimal_orchestrator(self, mock_components):
        """Test orchestrator with only FillProcessor."""
        orch = BotOrchestrator(
            coin="TEST",
            fill_processor=mock_components["fill_processor"],
        )
        
        fill = {"tid": 1, "px": "100.0", "sz": "1.0", "side": "B"}
        result = await orch.process_fill(fill=fill, position=0.0)
        
        assert result.success is True
        assert len(mock_components["fill_processor"].fills_processed) == 1


class TestOrchestratorFactory:
    """Factory method tests."""
    
    def test_create_minimal(self, mock_components):
        """Test minimal factory method."""
        orch = OrchestratorFactory.create_minimal(
            coin="TEST",
            fill_processor=mock_components["fill_processor"],
        )
        
        assert orch.coin == "TEST"
        assert orch.fill_processor is not None
        assert orch.state_manager is None
        assert orch.config.use_state_manager is False
    
    def test_create_full(self, mock_components):
        """Test full factory method."""
        orch = OrchestratorFactory.create_full(
            coin="TEST",
            fill_processor=mock_components["fill_processor"],
            state_manager=mock_components["state_manager"],
            execution_gateway=mock_components["execution_gateway"],
            grid_builder=mock_components["grid_builder"],
        )
        
        assert orch.coin == "TEST"
        assert orch.fill_processor is not None
        assert orch.state_manager is not None
        assert orch.execution_gateway is not None
        assert orch.grid_builder is not None
        assert orch.config.use_state_manager is True
        assert orch.config.use_execution_gateway is True
        assert orch.config.use_grid_builder is True


class TestCycleTimings:
    """Tests for cycle timing logic."""
    
    @pytest.mark.asyncio
    async def test_state_save_respects_interval(self, orchestrator):
        """Test state save respects interval."""
        orch, components = orchestrator
        
        # First cycle should trigger save (interval is 1 sec, last save is 0)
        # We need to ensure enough time has passed
        orch._last_state_save = 0.0
        orch.config.state_save_interval_sec = 0.0  # Always save
        
        await orch.cycle(mid=100.0, position=0.0)
        
        # State manager should have been called
        assert components["state_manager"].save_called is True
    
    @pytest.mark.asyncio
    async def test_pnl_log_respects_interval(self, orchestrator):
        """Test PnL logging respects interval."""
        orch, components = orchestrator
        
        # Configure for immediate PnL logging
        orch._last_pnl_log = 0.0
        orch.config.pnl_log_interval_sec = 0.0
        
        log_events = []
        orch._log_event = lambda e, **k: log_events.append(e)
        
        await orch.cycle(mid=100.0, position=0.0)
        
        assert "pnl_summary" in log_events
