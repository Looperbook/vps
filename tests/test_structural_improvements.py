"""
Tests for S-1, S-2, S-3 Structural Improvements.

S-1: Shadow Ledger - Position tracking with exchange reconciliation
S-2: Event Bus - Pub/sub for decoupled component communication  
S-3: Order Sync - OrderManager/OrderStateMachine synchronization
"""

import asyncio
import time
import pytest

from src.shadow_ledger import ShadowLedger, PendingOrder, ReconcileResult, LedgerSnapshot
from src.event_bus import EventBus, Event, EventType, Subscription
from src.order_sync import OrderSync, OrderSyncError
from src.order_manager import OrderManager
from src.order_state_machine import OrderStateMachine, OrderState
from src.strategy import GridLevel


# =============================================================================
# S-1: Shadow Ledger Tests
# =============================================================================

class TestShadowLedger:
    """Test cases for ShadowLedger."""
    
    @pytest.fixture
    def ledger(self):
        """Create fresh ledger for each test."""
        return ShadowLedger(coin="BTC")
    
    @pytest.mark.asyncio
    async def test_initial_state(self, ledger):
        """Test ledger initializes with zero positions."""
        assert ledger.confirmed_position == 0.0
        assert ledger.local_position == 0.0
        assert ledger.pending_exposure == 0.0
        assert ledger.effective_position == 0.0
    
    @pytest.mark.asyncio
    async def test_add_pending_order(self, ledger):
        """Test adding pending orders updates exposure."""
        await ledger.add_pending_order("cloid1", "buy", 1.0, 100.0)
        
        assert ledger.pending_exposure == 1.0
        assert ledger.effective_position == 1.0
        
        await ledger.add_pending_order("cloid2", "sell", 0.5, 101.0)
        
        assert ledger.pending_exposure == 0.5  # 1.0 - 0.5
        assert ledger.effective_position == 0.5
    
    @pytest.mark.asyncio
    async def test_update_pending_oid(self, ledger):
        """Test updating oid on pending order."""
        await ledger.add_pending_order("cloid1", "buy", 1.0, 100.0)
        
        result = await ledger.update_pending_oid("cloid1", 12345)
        assert result is True
        
        # Should be findable by oid now
        pending = await ledger.get_pending_order(oid=12345)
        assert pending is not None
        assert pending.cloid == "cloid1"
    
    @pytest.mark.asyncio
    async def test_remove_pending_by_cloid(self, ledger):
        """Test removing pending order by cloid."""
        await ledger.add_pending_order("cloid1", "buy", 1.0, 100.0)
        
        removed = await ledger.remove_pending_order(cloid="cloid1")
        
        assert removed is not None
        assert removed.cloid == "cloid1"
        assert ledger.pending_exposure == 0.0
    
    @pytest.mark.asyncio
    async def test_remove_pending_by_oid(self, ledger):
        """Test removing pending order by oid."""
        await ledger.add_pending_order("cloid1", "buy", 1.0, 100.0, oid=12345)
        
        removed = await ledger.remove_pending_order(oid=12345)
        
        assert removed is not None
        assert removed.oid == 12345
        assert ledger.pending_exposure == 0.0
    
    @pytest.mark.asyncio
    async def test_apply_fill_updates_position(self, ledger):
        """Test fills update local position."""
        await ledger.apply_fill("buy", 1.0)
        assert ledger.local_position == 1.0
        
        await ledger.apply_fill("sell", 0.5)
        assert ledger.local_position == 0.5
        
        await ledger.apply_fill("sell", 0.5)
        assert ledger.local_position == 0.0
    
    @pytest.mark.asyncio
    async def test_apply_fill_removes_pending(self, ledger):
        """Test fill removes corresponding pending order."""
        await ledger.add_pending_order("cloid1", "buy", 1.0, 100.0)
        assert ledger.pending_exposure == 1.0
        
        await ledger.apply_fill("buy", 1.0, cloid="cloid1")
        
        assert ledger.local_position == 1.0
        assert ledger.pending_exposure == 0.0
    
    @pytest.mark.asyncio
    async def test_reconcile_no_drift(self, ledger):
        """Test reconciliation with no drift."""
        await ledger.apply_fill("buy", 1.0)
        
        result = await ledger.reconcile(exchange_position=1.0)
        
        assert result.drift == 0.0
        assert result.is_significant is False
        assert ledger.confirmed_position == 1.0
    
    @pytest.mark.asyncio
    async def test_reconcile_with_drift(self, ledger):
        """Test reconciliation detects drift."""
        await ledger.apply_fill("buy", 1.0)
        
        # Exchange shows different position
        result = await ledger.reconcile(exchange_position=1.5)
        
        assert result.drift == 0.5
        assert result.local_position == 1.0
        assert result.exchange_position == 1.5
        # After reconcile, local should match exchange
        assert ledger.local_position == 1.5
        assert ledger.confirmed_position == 1.5
    
    @pytest.mark.asyncio
    async def test_reconcile_significant_drift_alert(self, ledger):
        """Test significant drift triggers callback."""
        drift_alerts = []
        
        def on_drift(result):
            drift_alerts.append(result)
        
        ledger._on_drift = on_drift
        
        await ledger.apply_fill("buy", 1.0)
        # 20% drift should trigger alert (threshold is 10%)
        await ledger.reconcile(exchange_position=1.2)
        
        assert len(drift_alerts) == 1
        assert drift_alerts[0].is_significant is True
    
    @pytest.mark.asyncio
    async def test_get_snapshot(self, ledger):
        """Test snapshot captures consistent state."""
        await ledger.apply_fill("buy", 1.0)
        await ledger.add_pending_order("cloid1", "sell", 0.5, 101.0)
        await ledger.reconcile(1.0)
        
        snapshot = await ledger.get_snapshot()
        
        assert isinstance(snapshot, LedgerSnapshot)
        assert snapshot.local_position == 1.0
        assert snapshot.pending_exposure == -0.5
        assert snapshot.effective_position == 0.5
        assert snapshot.pending_count == 1
    
    @pytest.mark.asyncio
    async def test_stale_pending_detection(self, ledger):
        """Test detection of stale pending orders."""
        await ledger.add_pending_order("cloid1", "buy", 1.0, 100.0)
        
        # Immediately check - should not be stale (1 second threshold)
        stale = await ledger.get_stale_pending_orders(max_age_ms=1000)
        assert len(stale) == 0
        
        # Manually age the order by modifying its timestamp
        pending = await ledger.get_pending_order(cloid="cloid1")
        pending.created_ms = pending.created_ms - 2000  # 2 seconds ago
        
        # Now it should be stale with 1 second threshold
        stale = await ledger.get_stale_pending_orders(max_age_ms=1000)
        assert len(stale) == 1
        assert stale[0].cloid == "cloid1"
    
    @pytest.mark.asyncio
    async def test_prune_stale_pending(self, ledger):
        """Test pruning stale pending orders."""
        await ledger.add_pending_order("cloid1", "buy", 1.0, 100.0)
        await ledger.add_pending_order("cloid2", "buy", 2.0, 101.0)
        
        # Manually age the orders
        for cloid in ["cloid1", "cloid2"]:
            pending = await ledger.get_pending_order(cloid=cloid)
            pending.created_ms = pending.created_ms - 2000  # 2 seconds ago
        
        # Prune with 1 second threshold (both are stale)
        count = await ledger.prune_stale_pending(max_age_ms=1000)
        
        assert count == 2
        assert ledger.pending_exposure == 0.0
    
    @pytest.mark.asyncio
    async def test_state_persistence(self, ledger):
        """Test state save/load."""
        await ledger.apply_fill("buy", 1.5)
        await ledger.reconcile(1.5)
        
        # Save state
        state = ledger.get_state()
        
        # Create new ledger and load
        new_ledger = ShadowLedger(coin="BTC")
        new_ledger.load_state(state)
        
        assert new_ledger.confirmed_position == 1.5
        assert new_ledger.local_position == 1.5
    
    @pytest.mark.asyncio
    async def test_stats_tracking(self, ledger):
        """Test statistics are tracked."""
        await ledger.add_pending_order("cloid1", "buy", 1.0, 100.0)
        await ledger.remove_pending_order(cloid="cloid1")  # Explicit remove
        await ledger.apply_fill("buy", 1.0)  # Fill without pending
        await ledger.reconcile(1.0)
        
        stats = ledger.get_stats()
        
        assert stats["total_fills"] == 1
        assert stats["total_reconciles"] == 1
        assert stats["pending_orders_added"] == 1
        assert stats["pending_orders_removed"] == 1


# =============================================================================
# S-2: Event Bus Tests
# =============================================================================

class TestEventBus:
    """Test cases for EventBus."""
    
    @pytest.fixture
    def bus(self):
        """Create fresh event bus for each test."""
        return EventBus(history_size=100)
    
    @pytest.mark.asyncio
    async def test_subscribe_and_publish(self, bus):
        """Test basic subscribe and publish."""
        received = []
        
        async def handler(event):
            received.append(event)
        
        bus.subscribe(EventType.FILL_RECEIVED, handler)
        
        event = Event(
            type=EventType.FILL_RECEIVED,
            data={"size": 1.0, "price": 100.0},
        )
        await bus.publish(event)
        
        # Process the event
        await bus.drain()
        
        assert len(received) == 1
        assert received[0].data["size"] == 1.0
    
    @pytest.mark.asyncio
    async def test_multiple_subscribers(self, bus):
        """Test multiple handlers for same event type."""
        received1 = []
        received2 = []
        
        async def handler1(event):
            received1.append(event)
        
        async def handler2(event):
            received2.append(event)
        
        bus.subscribe(EventType.FILL_RECEIVED, handler1)
        bus.subscribe(EventType.FILL_RECEIVED, handler2)
        
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.drain()
        
        assert len(received1) == 1
        assert len(received2) == 1
    
    @pytest.mark.asyncio
    async def test_priority_ordering(self, bus):
        """Test handlers called in priority order."""
        order = []
        
        async def low_priority(event):
            order.append("low")
        
        async def high_priority(event):
            order.append("high")
        
        bus.subscribe(EventType.FILL_RECEIVED, low_priority, priority=0)
        bus.subscribe(EventType.FILL_RECEIVED, high_priority, priority=10)
        
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.drain()
        
        assert order == ["high", "low"]
    
    @pytest.mark.asyncio
    async def test_global_subscriber(self, bus):
        """Test global subscriber receives all events."""
        received = []
        
        async def global_handler(event):
            received.append(event.type)
        
        bus.subscribe_all(global_handler)
        
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.publish(Event(type=EventType.ORDER_SUBMITTED, data={}))
        await bus.drain()
        
        assert EventType.FILL_RECEIVED in received
        assert EventType.ORDER_SUBMITTED in received
    
    @pytest.mark.asyncio
    async def test_filter_function(self, bus):
        """Test filter function blocks events."""
        received = []
        
        async def handler(event):
            received.append(event)
        
        def only_buys(event):
            return event.data.get("side") == "buy"
        
        bus.subscribe(EventType.FILL_RECEIVED, handler, filter_fn=only_buys)
        
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={"side": "buy"}))
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={"side": "sell"}))
        await bus.drain()
        
        assert len(received) == 1
        assert received[0].data["side"] == "buy"
    
    @pytest.mark.asyncio
    async def test_sync_handler(self, bus):
        """Test synchronous handlers work."""
        received = []
        
        def sync_handler(event):
            received.append(event)
        
        bus.subscribe(EventType.FILL_RECEIVED, sync_handler)
        
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.drain()
        
        assert len(received) == 1
    
    @pytest.mark.asyncio
    async def test_handler_error_isolation(self, bus):
        """Test one handler error doesn't stop others."""
        received = []
        
        async def bad_handler(event):
            raise ValueError("Handler error")
        
        async def good_handler(event):
            received.append(event)
        
        # Bad handler has higher priority, runs first
        bus.subscribe(EventType.FILL_RECEIVED, bad_handler, priority=10)
        bus.subscribe(EventType.FILL_RECEIVED, good_handler, priority=0)
        
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.drain()
        
        # Good handler should still have been called
        assert len(received) == 1
        # Error should be tracked
        assert bus._stats["handler_errors"] == 1
    
    @pytest.mark.asyncio
    async def test_unsubscribe(self, bus):
        """Test unsubscribing removes handler."""
        received = []
        
        async def handler(event):
            received.append(event)
        
        sub = bus.subscribe(EventType.FILL_RECEIVED, handler)
        
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.drain()
        assert len(received) == 1
        
        # Unsubscribe
        result = bus.unsubscribe(EventType.FILL_RECEIVED, sub)
        assert result is True
        
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.drain()
        assert len(received) == 1  # Still 1, not 2
    
    @pytest.mark.asyncio
    async def test_event_history(self, bus):
        """Test event history is maintained."""
        for i in range(5):
            await bus.publish(Event(
                type=EventType.FILL_RECEIVED,
                data={"index": i},
            ))
        await bus.drain()
        
        history = bus.get_history()
        
        assert len(history) == 5
        assert history[0].data["index"] == 0
        assert history[4].data["index"] == 4
    
    @pytest.mark.asyncio
    async def test_history_filter_by_type(self, bus):
        """Test history filtering by event type."""
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.publish(Event(type=EventType.ORDER_SUBMITTED, data={}))
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.drain()
        
        fill_history = bus.get_history(event_type=EventType.FILL_RECEIVED)
        
        assert len(fill_history) == 2
    
    @pytest.mark.asyncio
    async def test_emit_convenience(self, bus):
        """Test emit convenience method."""
        received = []
        
        async def handler(event):
            received.append(event)
        
        bus.subscribe(EventType.FILL_RECEIVED, handler)
        
        await bus.emit(EventType.FILL_RECEIVED, source="test", size=1.0, price=100.0)
        await bus.drain()
        
        assert len(received) == 1
        assert received[0].data["size"] == 1.0
        assert received[0].source == "test"
    
    @pytest.mark.asyncio
    async def test_stats_tracking(self, bus):
        """Test statistics are tracked."""
        async def handler(event):
            pass
        
        bus.subscribe(EventType.FILL_RECEIVED, handler)
        
        for _ in range(10):
            await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        await bus.drain()
        
        stats = bus.get_stats()
        
        assert stats["events_published"] == 10
        assert stats["events_processed"] == 10
    
    @pytest.mark.asyncio
    async def test_start_stop(self, bus):
        """Test start/stop lifecycle."""
        received = []
        
        async def handler(event):
            received.append(event)
        
        bus.subscribe(EventType.FILL_RECEIVED, handler)
        
        # Start in background
        task = asyncio.create_task(bus.start())
        
        # Give it time to start
        await asyncio.sleep(0.1)
        assert bus._running is True
        
        # Publish event
        await bus.publish(Event(type=EventType.FILL_RECEIVED, data={}))
        
        # Give it time to process
        await asyncio.sleep(0.1)
        
        # Stop
        bus.stop()
        await asyncio.sleep(0.1)
        
        # Cancel task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        
        assert len(received) == 1


# =============================================================================
# S-3: Order Sync Tests
# =============================================================================

class TestOrderSync:
    """Test cases for OrderSync."""
    
    @pytest.fixture
    def sync(self):
        """Create OrderSync with fresh manager and state machine."""
        manager = OrderManager(tick_sz=0.01, px_decimals=2)
        state_machine = OrderStateMachine()
        return OrderSync(manager, state_machine)
    
    def _make_level(self, side="buy", price=100.0, size=1.0):
        """Helper to create grid level."""
        return GridLevel(side=side, px=price, sz=size)
    
    def test_register_order(self, sync):
        """Test order registration updates both systems."""
        level = self._make_level()
        
        active, state = sync.register_order(level, cloid="cloid1")
        
        assert active is not None
        assert active.cloid == "cloid1"
        assert state is not None
        assert state.state == OrderState.PENDING
        
        # Verify in both systems
        assert sync.order_manager.lookup(cloid="cloid1") is not None
        assert sync.order_state_machine.get_order(cloid="cloid1") is not None
    
    def test_acknowledge_order(self, sync):
        """Test acknowledging order updates both systems."""
        level = self._make_level()
        sync.register_order(level, cloid="cloid1")
        
        result = sync.acknowledge_order("cloid1", new_oid=12345)
        
        assert result is True
        
        # Check manager has oid
        active = sync.order_manager.lookup(cloid="cloid1")
        assert active.oid == 12345
        
        # Check state machine transitioned
        state = sync.order_state_machine.get_order(cloid="cloid1")
        assert state.state == OrderState.OPEN
        assert state.oid == 12345
    
    def test_record_fill_complete(self, sync):
        """Test recording complete fill."""
        level = self._make_level(size=1.0)
        sync.register_order(level, cloid="cloid1", oid=12345)
        sync.acknowledge_order("cloid1")
        
        active = sync.record_fill("cloid1", 12345, fill_sz=1.0, is_complete=True)
        
        assert active is not None
        
        # Should be removed from manager (complete)
        assert sync.order_manager.lookup(cloid="cloid1") is None
        
        # State machine should show FILLED
        state = sync.order_state_machine.get_order(cloid="cloid1")
        assert state.state == OrderState.FILLED
    
    def test_record_fill_partial(self, sync):
        """Test recording partial fill."""
        level = self._make_level(size=2.0)
        sync.register_order(level, cloid="cloid1", oid=12345)
        sync.acknowledge_order("cloid1")
        
        active = sync.record_fill("cloid1", 12345, fill_sz=1.0, is_complete=False)
        
        assert active is not None
        
        # Should still be in manager (partial)
        assert sync.order_manager.lookup(cloid="cloid1") is not None
        
        # State machine should show PARTIALLY_FILLED
        state = sync.order_state_machine.get_order(cloid="cloid1")
        assert state.state == OrderState.PARTIALLY_FILLED
    
    def test_cancel_order(self, sync):
        """Test cancelling order updates both systems."""
        level = self._make_level()
        sync.register_order(level, cloid="cloid1", oid=12345)
        sync.acknowledge_order("cloid1")
        
        active = sync.cancel_order("cloid1", 12345)
        
        assert active is not None
        
        # Should be removed from manager
        assert sync.order_manager.lookup(cloid="cloid1") is None
        
        # State machine should show CANCELLED
        state = sync.order_state_machine.get_order(cloid="cloid1")
        assert state.state == OrderState.CANCELLED
    
    def test_reject_order(self, sync):
        """Test rejecting order updates both systems."""
        level = self._make_level()
        sync.register_order(level, cloid="cloid1")
        
        active = sync.reject_order("cloid1", error_code="INVALID", error_message="Bad order")
        
        # Should be removed from manager
        assert sync.order_manager.lookup(cloid="cloid1") is None
        
        # State machine should show REJECTED with error info
        state = sync.order_state_machine.get_order(cloid="cloid1")
        assert state.state == OrderState.REJECTED
        assert state.error_code == "INVALID"
    
    def test_validate_consistency_clean(self, sync):
        """Test consistency check on clean state."""
        level = self._make_level()
        sync.register_order(level, cloid="cloid1")
        sync.acknowledge_order("cloid1")
        
        errors = sync.validate_consistency()
        
        assert len(errors) == 0
    
    def test_validate_consistency_manager_only(self, sync):
        """Test detecting order in manager but not state machine."""
        level = self._make_level()
        
        # Register directly in manager, bypassing sync
        sync.order_manager.register(level, "orphan_cloid", None)
        
        errors = sync.validate_consistency()
        
        assert len(errors) == 1
        assert errors[0]["type"] == "manager_only"
        assert errors[0]["id_val"] == "orphan_cloid"
    
    def test_validate_consistency_terminal_in_manager(self, sync):
        """Test detecting terminal order still in manager."""
        level = self._make_level()
        sync.register_order(level, cloid="cloid1", oid=12345)
        sync.acknowledge_order("cloid1")
        
        # Cancel in state machine only, leaving in manager
        sync.order_state_machine.cancel(cloid="cloid1")
        
        errors = sync.validate_consistency()
        
        assert len(errors) >= 1
        terminal_errors = [e for e in errors if e["type"] == "terminal_in_manager"]
        # May find 2 errors (one for cloid, one for oid) pointing to same order
        assert len(terminal_errors) >= 1
        # Verify it's the right order
        assert any(e["id_val"] == "cloid1" or e["id_val"] == 12345 for e in terminal_errors)
    
    def test_reconcile_fixes_inconsistencies(self, sync):
        """Test reconcile fixes detected issues."""
        level = self._make_level()
        sync.register_order(level, cloid="cloid1", oid=12345)
        sync.acknowledge_order("cloid1")
        
        # Cancel in state machine only
        sync.order_state_machine.cancel(cloid="cloid1")
        
        # Should have inconsistency
        errors_before = sync.validate_consistency()
        assert len(errors_before) > 0
        
        # Reconcile
        corrections = sync.reconcile()
        assert corrections > 0
        
        # Should be clean now
        errors_after = sync.validate_consistency()
        assert len(errors_after) == 0
    
    def test_get_order(self, sync):
        """Test getting order from both systems."""
        level = self._make_level()
        sync.register_order(level, cloid="cloid1")
        
        result = sync.get_order(cloid="cloid1")
        
        assert result is not None
        active, state = result
        assert active is not None
        assert state is not None
    
    def test_get_stats(self, sync):
        """Test statistics tracking."""
        level = self._make_level()
        sync.register_order(level, cloid="cloid1")
        sync.acknowledge_order("cloid1")
        sync.record_fill("cloid1", None, fill_sz=1.0, is_complete=True)
        
        stats = sync.get_stats()
        
        assert stats["orders_registered"] == 1
        assert stats["orders_acknowledged"] == 1
        assert stats["orders_filled"] == 1
    
    def test_clear(self, sync):
        """Test clearing both systems."""
        level = self._make_level()
        sync.register_order(level, cloid="cloid1")
        
        sync.clear()
        
        assert sync.order_manager.open_count() == 0
        assert len(sync.order_state_machine.get_active_orders()) == 0


# =============================================================================
# Integration Tests
# =============================================================================

class TestStructuralIntegration:
    """Integration tests combining structural improvements."""
    
    @pytest.mark.asyncio
    async def test_shadow_ledger_with_event_bus(self):
        """Test shadow ledger publishing events to event bus."""
        ledger = ShadowLedger(coin="ETH")
        bus = EventBus()
        
        fills_received = []
        
        async def on_fill(event):
            fills_received.append(event)
        
        bus.subscribe(EventType.FILL_RECEIVED, on_fill)
        
        # Simulate fill with event publishing
        async def fill_with_event(side, size, price):
            await ledger.apply_fill(side, size)
            await bus.emit(
                EventType.FILL_RECEIVED,
                source="ledger",
                side=side,
                size=size,
                price=price,
                position=ledger.local_position,
            )
        
        await fill_with_event("buy", 1.0, 2000.0)
        await fill_with_event("sell", 0.5, 2010.0)
        await bus.drain()
        
        assert len(fills_received) == 2
        assert ledger.local_position == 0.5
    
    @pytest.mark.asyncio
    async def test_order_sync_with_event_bus(self):
        """Test order sync publishing events to event bus."""
        manager = OrderManager(tick_sz=0.01, px_decimals=2)
        state_machine = OrderStateMachine()
        sync = OrderSync(manager, state_machine)
        bus = EventBus()
        
        events_received = []
        
        async def on_order_event(event):
            events_received.append(event)
        
        bus.subscribe(EventType.ORDER_SUBMITTED, on_order_event)
        bus.subscribe(EventType.ORDER_ACKNOWLEDGED, on_order_event)
        bus.subscribe(EventType.ORDER_FILLED, on_order_event)
        
        # Register order with event
        level = GridLevel(side="buy", px=100.0, sz=1.0)
        sync.register_order(level, cloid="cloid1")
        await bus.emit(EventType.ORDER_SUBMITTED, cloid="cloid1", side="buy", price=100.0)
        
        # Acknowledge
        sync.acknowledge_order("cloid1", new_oid=12345)
        await bus.emit(EventType.ORDER_ACKNOWLEDGED, cloid="cloid1", oid=12345)
        
        # Fill
        sync.record_fill("cloid1", 12345, fill_sz=1.0, is_complete=True)
        await bus.emit(EventType.ORDER_FILLED, cloid="cloid1", oid=12345, size=1.0)
        
        await bus.drain()
        
        assert len(events_received) == 3
        types = [e.type for e in events_received]
        assert EventType.ORDER_SUBMITTED in types
        assert EventType.ORDER_ACKNOWLEDGED in types
        assert EventType.ORDER_FILLED in types
    
    @pytest.mark.asyncio
    async def test_full_order_lifecycle(self):
        """Test complete order lifecycle across all systems."""
        # Create all components
        ledger = ShadowLedger(coin="BTC")
        bus = EventBus()
        manager = OrderManager(tick_sz=0.01, px_decimals=2)
        state_machine = OrderStateMachine()
        sync = OrderSync(manager, state_machine)
        
        position_events = []
        
        async def track_position(event):
            position_events.append(event.data.get("position", 0))
        
        bus.subscribe(EventType.POSITION_CHANGED, track_position)
        
        # 1. Submit order
        level = GridLevel(side="buy", px=50000.0, sz=0.01)
        sync.register_order(level, cloid="btc_buy_1")
        await ledger.add_pending_order("btc_buy_1", "buy", 0.01, 50000.0)
        
        # Verify pending state
        assert ledger.pending_exposure == 0.01
        state = sync.order_state_machine.get_order(cloid="btc_buy_1")
        assert state.state == OrderState.PENDING
        
        # 2. Exchange acknowledges
        sync.acknowledge_order("btc_buy_1", new_oid=999)
        await ledger.update_pending_oid("btc_buy_1", 999)
        
        state = sync.order_state_machine.get_order(cloid="btc_buy_1")
        assert state.state == OrderState.OPEN
        
        # 3. Fill received
        sync.record_fill("btc_buy_1", 999, fill_sz=0.01, is_complete=True)
        await ledger.apply_fill("buy", 0.01, cloid="btc_buy_1")
        await bus.emit(EventType.POSITION_CHANGED, position=ledger.local_position)
        await bus.drain()
        
        # Verify final state
        assert ledger.local_position == 0.01
        assert ledger.pending_exposure == 0.0
        state = sync.order_state_machine.get_order(cloid="btc_buy_1")
        assert state.state == OrderState.FILLED
        assert sync.order_manager.lookup(cloid="btc_buy_1") is None
        
        # 4. Reconcile with exchange
        result = await ledger.reconcile(0.01)
        assert result.drift == 0.0
        
        # Verify position event was published
        assert len(position_events) == 1
        assert position_events[0] == 0.01
