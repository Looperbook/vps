"""
Tests for StateManager - centralized state ownership module.

Tests cover:
- Position management
- PnL tracking (session vs all-time)
- Fill timestamp handling
- State persistence
- Atomic snapshots
- Fill application
- Position reconciliation
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock

from src.state.state_manager import StateManager, StateManagerConfig, StateSnapshot


@dataclass
class MockPositionTracker:
    """Mock position tracker for testing."""
    position: float = 0.0
    realized_pnl: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class MockShadowLedger:
    """Mock shadow ledger for testing."""
    local_position: float = 0.0
    confirmed_position: float = 0.0


@dataclass
class MockRiskEngine:
    """Mock risk engine for testing."""
    _position: float = 0.0
    
    def set_position(self, pos: float) -> None:
        self._position = pos


class MockStateStore:
    """Mock state store for testing."""
    def __init__(self, initial_data: Optional[Dict[str, Any]] = None):
        self._data = initial_data or {}
        self.save_called = False
        self.save_data = None
        
    async def save(self, data: Dict[str, Any]) -> None:
        self.save_called = True
        self.save_data = data
        self._data = data
        
    async def load(self) -> Dict[str, Any]:
        return self._data.copy()


@pytest.fixture
def mock_dependencies():
    """Create all mock dependencies for StateManager."""
    return {
        "state_store": MockStateStore(),
        "position_tracker": MockPositionTracker(),
        "shadow_ledger": MockShadowLedger(),
        "risk_engine": MockRiskEngine(),
    }


@pytest.fixture
def state_manager(mock_dependencies):
    """Create StateManager with mock dependencies."""
    config = StateManagerConfig(fill_rescan_ms=60000)
    return StateManager(
        coin="TEST",
        state_store=mock_dependencies["state_store"],
        position_tracker=mock_dependencies["position_tracker"],
        shadow_ledger=mock_dependencies["shadow_ledger"],
        risk_engine=mock_dependencies["risk_engine"],
        config=config,
    ), mock_dependencies


class TestStateManagerPosition:
    """Position management tests."""
    
    @pytest.mark.asyncio
    async def test_position_property(self, state_manager):
        """Test position property reads from tracker."""
        sm, deps = state_manager
        deps["position_tracker"].position = 5.0
        
        assert sm.position == 5.0
        
    @pytest.mark.asyncio
    async def test_set_position_updates_all_components(self, state_manager):
        """Test set_position updates tracker, ledger, and risk."""
        sm, deps = state_manager
        
        await sm.set_position(10.0, source="test")
        
        assert deps["position_tracker"].position == 10.0
        assert deps["shadow_ledger"].local_position == 10.0
        assert deps["risk_engine"]._position == 10.0


class TestStateManagerPnL:
    """PnL tracking tests."""
    
    @pytest.mark.asyncio
    async def test_session_pnl_starts_zero(self, state_manager):
        """Test session PnL starts at zero."""
        sm, deps = state_manager
        assert sm.session_realized_pnl == 0.0
        
    @pytest.mark.asyncio
    async def test_add_realized_pnl_updates_both(self, state_manager):
        """Test add_realized_pnl updates session and all-time."""
        sm, deps = state_manager
        
        await sm.add_realized_pnl(100.0)
        
        assert sm.session_realized_pnl == 100.0
        assert sm.alltime_realized_pnl == 100.0
        assert deps["position_tracker"].realized_pnl == 100.0
        
    @pytest.mark.asyncio
    async def test_add_realized_pnl_accumulates(self, state_manager):
        """Test PnL accumulates correctly."""
        sm, deps = state_manager
        
        await sm.add_realized_pnl(50.0)
        await sm.add_realized_pnl(30.0)
        await sm.add_realized_pnl(-10.0)
        
        assert sm.session_realized_pnl == 70.0
        assert sm.alltime_realized_pnl == 70.0


class TestStateManagerFillTracking:
    """Fill timestamp tracking tests."""
    
    @pytest.mark.asyncio
    async def test_last_fill_time_starts_zero(self, state_manager):
        """Test last_fill_time starts at zero."""
        sm, deps = state_manager
        assert sm.last_fill_time_ms == 0
        
    @pytest.mark.asyncio
    async def test_update_last_fill_time_max(self, state_manager):
        """Test update_last_fill_time uses max."""
        sm, deps = state_manager
        
        await sm.update_last_fill_time(1000)
        assert sm.last_fill_time_ms == 1000
        
        await sm.update_last_fill_time(500)  # Older - should not update
        assert sm.last_fill_time_ms == 1000
        
        await sm.update_last_fill_time(2000)  # Newer
        assert sm.last_fill_time_ms == 2000


class TestStateManagerSnapshot:
    """Atomic snapshot tests."""
    
    @pytest.mark.asyncio
    async def test_get_snapshot_returns_all_state(self, state_manager):
        """Test snapshot contains all state."""
        sm, deps = state_manager
        
        deps["position_tracker"].position = 5.0
        await sm.add_realized_pnl(100.0)
        await sm.update_last_fill_time(5000)
        await sm.set_grid_center(150.0)
        
        snapshot = await sm.get_snapshot()
        
        assert snapshot.position == 5.0
        assert snapshot.session_realized_pnl == 100.0
        assert snapshot.alltime_realized_pnl == 100.0
        assert snapshot.last_fill_time_ms == 5000
        assert snapshot.grid_center == 150.0
        assert snapshot.timestamp_ms > 0
        
    @pytest.mark.asyncio
    async def test_snapshot_to_dict(self, state_manager):
        """Test snapshot serialization."""
        sm, deps = state_manager
        
        deps["position_tracker"].position = 3.0
        await sm.add_realized_pnl(50.0)
        
        snapshot = await sm.get_snapshot()
        data = snapshot.to_dict()
        
        assert data["position"] == 3.0
        assert data["realized_pnl"] == 50.0  # All-time
        assert "state_version" in data
        
    @pytest.mark.asyncio
    async def test_snapshot_from_dict(self):
        """Test snapshot deserialization."""
        data = {
            "position": 10.0,
            "realized_pnl": 200.0,
            "last_fill_time_ms": 9000,
            "grid_center": 175.0,
            "state_version": 3,
        }
        
        snapshot = StateSnapshot.from_dict(data, session_start=1000.0)
        
        assert snapshot.position == 10.0
        assert snapshot.alltime_realized_pnl == 200.0
        assert snapshot.session_realized_pnl == 0.0  # Always 0 on load
        assert snapshot.last_fill_time_ms == 9000
        assert snapshot.grid_center == 175.0


class TestStateManagerApplyFill:
    """Fill application tests."""
    
    @pytest.mark.asyncio
    async def test_apply_fill_buy(self, state_manager):
        """Test applying a buy fill."""
        sm, deps = state_manager
        
        await sm.apply_fill(
            side="buy",
            size=2.0,
            pnl=10.0,
            timestamp_ms=1000,
        )
        
        assert deps["position_tracker"].position == 2.0
        assert sm.session_realized_pnl == 10.0
        assert sm.last_fill_time_ms == 1000
        
    @pytest.mark.asyncio
    async def test_apply_fill_sell(self, state_manager):
        """Test applying a sell fill."""
        sm, deps = state_manager
        deps["position_tracker"].position = 5.0
        
        await sm.apply_fill(
            side="sell",
            size=2.0,
            pnl=20.0,
            timestamp_ms=2000,
        )
        
        assert deps["position_tracker"].position == 3.0
        assert sm.session_realized_pnl == 20.0
        
    @pytest.mark.asyncio
    async def test_apply_fill_updates_all_components(self, state_manager):
        """Test apply_fill updates all dependent components."""
        sm, deps = state_manager
        
        await sm.apply_fill(
            side="buy",
            size=1.0,
            pnl=5.0,
            timestamp_ms=1500,
        )
        
        assert deps["shadow_ledger"].local_position == 1.0
        assert deps["risk_engine"]._position == 1.0
        assert deps["position_tracker"].realized_pnl == 5.0


class TestStateManagerPersistence:
    """State persistence tests."""
    
    @pytest.mark.asyncio
    async def test_persist_saves_state(self, state_manager):
        """Test persist saves current state."""
        sm, deps = state_manager
        store = deps["state_store"]
        
        deps["position_tracker"].position = 7.0
        await sm.add_realized_pnl(150.0)
        await sm.update_last_fill_time(8000)
        
        await sm.persist()
        
        assert store.save_called is True
        assert store.save_data["position"] == 7.0
        assert store.save_data["realized_pnl"] == 150.0
        assert store.save_data["last_fill_time_ms"] == 8000
        
    @pytest.mark.asyncio
    async def test_load_restores_state(self, mock_dependencies):
        """Test load restores state from storage."""
        store = MockStateStore({
            "position": 12.0,
            "realized_pnl": 300.0,
            "last_fill_time_ms": 5000,
            "grid_center": 200.0,
        })
        mock_dependencies["state_store"] = store
        
        config = StateManagerConfig(fill_rescan_ms=1000)
        sm = StateManager(
            coin="TEST",
            config=config,
            **mock_dependencies,
        )
        
        # Load with current time far enough in the future
        current_ms = 10000
        snapshot = await sm.load(current_ms)
        
        assert mock_dependencies["position_tracker"].position == 12.0
        assert sm.alltime_realized_pnl == 300.0
        assert sm.session_realized_pnl == 0.0  # Reset on load
        assert sm.grid_center == 200.0
        
    @pytest.mark.asyncio
    async def test_load_adjusts_fill_time_with_rescan(self, mock_dependencies):
        """Test load applies rescan window to fill time."""
        store = MockStateStore({
            "position": 1.0,
            "last_fill_time_ms": 1000,  # Very old
        })
        mock_dependencies["state_store"] = store
        
        config = StateManagerConfig(fill_rescan_ms=5000)
        sm = StateManager(
            coin="TEST",
            config=config,
            **mock_dependencies,
        )
        
        current_ms = 100000
        await sm.load(current_ms)
        
        # Should be adjusted to current - rescan window
        assert sm.last_fill_time_ms == current_ms - 5000


class TestStateManagerReconciliation:
    """Position reconciliation tests."""
    
    @pytest.mark.asyncio
    async def test_reconcile_position_updates_all(self, state_manager):
        """Test reconcile updates all components."""
        sm, deps = state_manager
        deps["position_tracker"].position = 5.0
        deps["shadow_ledger"].local_position = 5.0
        
        drift = await sm.reconcile_position(7.0)
        
        assert drift == 2.0
        assert deps["position_tracker"].position == 7.0
        assert deps["shadow_ledger"].local_position == 7.0
        assert deps["shadow_ledger"].confirmed_position == 7.0
        assert deps["risk_engine"]._position == 7.0
        
    @pytest.mark.asyncio
    async def test_reconcile_no_drift(self, state_manager):
        """Test reconcile with no drift."""
        sm, deps = state_manager
        deps["position_tracker"].position = 5.0
        
        drift = await sm.reconcile_position(5.0)
        
        assert drift < 1e-9


class TestStateManagerFillProcessorSync:
    """FillProcessor synchronization tests."""
    
    @pytest.mark.asyncio
    async def test_get_state_for_fill_processor(self, state_manager):
        """Test get_state_for_fill_processor returns needed fields."""
        sm, deps = state_manager
        
        deps["position_tracker"].position = 3.0
        await sm.add_realized_pnl(75.0)
        await sm.update_last_fill_time(4000)
        
        state = sm.get_state_for_fill_processor()
        
        assert state["position"] == 3.0
        assert state["session_pnl"] == 75.0
        assert state["alltime_pnl"] == 75.0
        assert state["last_fill_time_ms"] == 4000
        
    @pytest.mark.asyncio
    async def test_sync_from_fill_processor(self, state_manager):
        """Test sync_from_fill_processor updates all state."""
        sm, deps = state_manager
        
        sm.sync_from_fill_processor(
            position=8.0,
            session_pnl=200.0,
            alltime_pnl=500.0,
            last_fill_time_ms=9000,
        )
        
        assert deps["position_tracker"].position == 8.0
        assert deps["position_tracker"].realized_pnl == 200.0
        assert sm.session_realized_pnl == 200.0
        assert sm.alltime_realized_pnl == 500.0
        assert sm.last_fill_time_ms == 9000
        assert deps["shadow_ledger"].local_position == 8.0
        assert deps["risk_engine"]._position == 8.0


class TestStateManagerSession:
    """Session management tests."""
    
    @pytest.mark.asyncio
    async def test_session_start_time_set(self, state_manager):
        """Test session start time is set on creation."""
        sm, deps = state_manager
        assert sm.session_start_time > 0
        
    @pytest.mark.asyncio
    async def test_session_duration(self, state_manager):
        """Test session duration calculation."""
        sm, deps = state_manager
        # Duration should be small since just created
        assert sm.session_duration_sec < 1.0
        
    @pytest.mark.asyncio
    async def test_reset_session(self, state_manager):
        """Test reset_session clears session state."""
        sm, deps = state_manager
        
        await sm.add_realized_pnl(100.0)
        assert sm.session_realized_pnl == 100.0
        
        await sm.reset_session()
        
        assert sm.session_realized_pnl == 0.0
        assert deps["position_tracker"].realized_pnl == 0.0
        # All-time should be preserved
        assert sm.alltime_realized_pnl == 100.0


class TestStateManagerGridState:
    """Grid state management tests."""
    
    @pytest.mark.asyncio
    async def test_grid_center_initially_none(self, state_manager):
        """Test grid_center starts as None."""
        sm, deps = state_manager
        assert sm.grid_center is None
        
    @pytest.mark.asyncio
    async def test_set_grid_center(self, state_manager):
        """Test setting grid center."""
        sm, deps = state_manager
        
        await sm.set_grid_center(150.0)
        
        assert sm.grid_center == 150.0
