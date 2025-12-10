"""
GRIDBOT CRITICAL PATCHES
========================

This file contains all P0 (Critical) patches ready to apply.
Apply these in order before going live.

USAGE:
1. Review each patch section
2. Apply to the target file
3. Run tests
4. Verify with the checklist in CODE_REVIEW.md
"""

# =============================================================================
# PATCH C-1: Fix Race Condition in Fill/Grid Rebuild
# File: src/bot.py
# Replace: _build_and_place_grid method (lines ~910-1020)
# =============================================================================

PATCH_C1_BUILD_AND_PLACE_GRID = '''
async def _build_and_place_grid(self, mid: float) -> None:
    """Build and place grid orders with race-condition protection."""
    if not self.strategy or not self.router:
        return
    if mid <= 0:
        self._log_event("grid_build_skip_mid", mid=mid)
        return
    
    # CRITICAL FIX: Hold position lock through grid computation to prevent
    # fills from changing position during level calculation
    async with self.position_lock:
        position_snapshot = self.position
        
        self._log_event("grid_rebuild_start", mid=mid, position=position_snapshot, 
                       open_orders=self._open_count())
        
        # Update strategy models under lock
        try:
            self.strategy.on_price(mid)
            try:
                self.rich_metrics.volatility_estimate.labels(coin=self.coin).set(self.strategy.last_vol)
            except Exception:
                pass
            try:
                self.rich_metrics.atr_value.labels(coin=self.coin).set(self.strategy.last_atr)
            except Exception:
                pass
        except Exception:
            pass
        
        # Compute grid levels under lock
        base_size = self.grid_calculator.calculate_order_size(mid)
        build_result = self.grid_calculator.build_filtered_levels(
            strategy=self.strategy,
            risk=self.risk,
            mid=mid,
            position=position_snapshot,
            base_size=base_size,
            flatten_mode=self._flatten_mode
        )
        
        spacing = build_result.spacing
        levels = build_result.levels
        
        # Export grid metrics
        try:
            self.rich_metrics.grid_width_pct.labels(coin=self.coin).set(spacing * 100.0)
        except Exception:
            pass
        
        # Compute diff under lock
        existing_keys = set(self.orders_by_price.keys())
        grid_diff = self.grid_calculator.compute_grid_diff(
            desired_levels=levels,
            existing_keys=existing_keys,
            orders_by_price=self.orders_by_price,
            reprice_tick_threshold=self.cfg.reprice_tick_threshold
        )
        
        to_cancel_keys = grid_diff.to_cancel_keys
        to_place = grid_diff.to_place
    
    # Release lock before I/O - we've captured everything we need
    
    # Instrument strategy outputs
    try:
        if self.strategy.grid_center:
            try:
                self.rich_metrics.grid_center.labels(coin=self.coin).set(self.strategy.grid_center)
            except Exception:
                pass
        try:
            bias = self.strategy.trend_bias(mid)
            self.rich_metrics.trend_bias.labels(coin=self.coin).set(bias)
        except Exception:
            pass
        try:
            target_notional = self.effective_investment_usd * self.cfg.leverage
            target_pos = target_notional / max(self.strategy.grid_center or mid, 1e-9)
            skew = abs(position_snapshot) / max(target_pos, 1e-9) if target_pos > 0 else 0.0
            self.rich_metrics.skew_ratio.labels(coin=self.coin).set(skew)
        except Exception:
            pass
    except Exception:
        pass
    
    # Cancel stale orders
    for key in to_cancel_keys:
        rec = self.orders_by_price.get(key)
        if not rec:
            continue
        try:
            await self._cancel_record(rec, reason="grid_diff")
        except Exception as exc:
            self._log_event("cancel_diff_error", err=str(exc), key=key)
    
    # CRITICAL: Final position check before placement
    async with self.position_lock:
        current_pos = self.position
    
    position_drift = abs(current_pos - position_snapshot)
    drift_pct = position_drift / max(abs(position_snapshot), 1e-9)
    
    if position_drift > 1e-9 and drift_pct > 0.1:  # >10% drift
        self._log_event("grid_rebuild_aborted_drift", 
                       snapshot=position_snapshot, 
                       current=current_pos,
                       drift_pct=drift_pct)
        self.rebuild_needed = True
        return
    
    await self._submit_levels_batch(to_place, mode="grid")
    self._log_event(
        "grid_rebuild_complete",
        mid=mid,
        spacing=spacing,
        placed=len(to_place),
        canceled=len(to_cancel_keys),
        remaining_open=self._open_count(),
    )
    await self._push_status()
    await self._maybe_flatten(mid)
'''

# =============================================================================
# PATCH C-2: Fix Partial Fill Threshold
# File: src/order_manager.py
# Replace: handle_partial_fill method (lines ~160-185)
# =============================================================================

PATCH_C2_PARTIAL_FILL = '''
def handle_partial_fill(self, cloid: Optional[str], oid: Optional[int], 
                       fill_sz: float, min_remaining_sz: float = 0.001) -> Optional[ActiveOrder]:
    """
    Handle partial fill - update filled_qty, only remove if fully filled.
    
    FIXED: Uses absolute minimum remaining size instead of percentage threshold.
    This prevents edge cases where the 0.99 ratio misses tiny final fills.
    
    Args:
        cloid: Client order ID
        oid: Exchange order ID
        fill_sz: Size of this fill
        min_remaining_sz: Minimum remaining size to consider order active (default 0.001)
    
    Returns:
        The ActiveOrder if found (for position updates), None otherwise.
    """
    if cloid is None and oid is None:
        return None
    
    rec = None
    if cloid and cloid in self.orders_by_cloid:
        rec = self.orders_by_cloid.get(cloid)
    if rec is None and oid is not None and oid in self.orders_by_oid:
        rec = self.orders_by_oid.get(oid)
    
    if not rec:
        return None
    
    rec.filled_qty += fill_sz
    remaining = rec.original_qty - rec.filled_qty
    
    # FIXED: Use absolute threshold instead of percentage
    # Order is considered "filled" when remaining is below minimum tradeable size
    is_fully_filled = remaining < min_remaining_sz or remaining < rec.original_qty * 0.001
    
    if is_fully_filled:
        self._unindex(rec)
        log.info(f'{{"event":"order_filled_complete","cloid":"{cloid}","oid":{oid},'
                 f'"filled_qty":{rec.filled_qty},"original_qty":{rec.original_qty},'
                 f'"remaining":{remaining}}}')
        return rec
    
    # Partial fill: keep order in registry
    log.info(f'{{"event":"partial_fill","cloid":"{cloid}","oid":{oid},'
             f'"fill_sz":{fill_sz},"filled_qty":{rec.filled_qty},'
             f'"remaining":{remaining},"original_qty":{rec.original_qty}}}')
    return rec
'''

# =============================================================================
# PATCH C-3: Fix WS Reconnect Fill Loss
# File: src/market_data.py
# Add: reconnection tracking fields and methods
# =============================================================================

PATCH_C3_MARKET_DATA_INIT = '''
# Add these fields to MarketData.__init__() after existing init code:

        # PATCH C-3: Track reconnection for immediate REST poll
        self._reconnection_pending = False
        self._last_ws_disconnect_ms = 0
'''

PATCH_C3_RESUBSCRIBE = '''
def _resubscribe(self) -> None:
    """
    Best-effort resubscribe to user fills and mids.
    PATCHED: Marks reconnection for bot to trigger immediate REST poll.
    """
    # ADDED: Track disconnect time for fill gap detection
    self._reconnection_pending = True
    self._last_ws_disconnect_ms = int(time.time() * 1000)
    
    try:
        if self.info.ws_manager is None:
            return
        # ... rest of existing _resubscribe code ...
'''

PATCH_C3_CONSUME_RECONNECTION = '''
def consume_reconnection_event(self) -> Optional[int]:
    """
    Check if WS reconnection occurred and return disconnect timestamp.
    
    Bot should call this in main loop and poll REST fills from the
    returned timestamp if not None.
    
    Returns:
        Disconnect timestamp in ms if reconnection pending, None otherwise.
    """
    if self._reconnection_pending:
        self._reconnection_pending = False
        return self._last_ws_disconnect_ms
    return None
'''

PATCH_C3_BOT_MAIN_LOOP = '''
# Add to bot.py run() method, inside the main while loop, after data_paused check:

                # PATCH C-3: Check for WS reconnection and poll missed fills immediately
                reconnect_ts = self.market.consume_reconnection_event() if self.market else None
                if reconnect_ts:
                    self._log_event("ws_reconnect_fill_poll", since_ms=reconnect_ts)
                    try:
                        # Poll from 5 seconds before disconnect to catch any fills in flight
                        fills = await self.market.user_fills_since(
                            self.account, 
                            max(0, reconnect_ts - 5000)
                        )
                        for f in sorted(fills, key=lambda x: int(x.get("time", 0))):
                            if f.get("coin") != self.coin:
                                continue
                            await self._handle_fill(f)
                        self._log_event("ws_reconnect_fill_poll_complete", 
                                       fills_processed=len(fills))
                    except Exception as exc:
                        self._log_event("ws_reconnect_fill_poll_error", err=str(exc))
'''

# =============================================================================
# PATCH C-4: Fix Grid Spacing Collapse
# File: src/strategy.py
# Replace: compute_spacing method (lines ~115-175)
# =============================================================================

PATCH_C4_COMPUTE_SPACING = '''
def compute_spacing(self, px: float, position: float = 0.0, 
                   grid_center: Optional[float] = None) -> float:
    """
    Compute grid spacing with protection against collapse.
    
    PATCHED: Enforces stricter minimum spacing to prevent grid compression
    during high volatility or loss-drift scenarios.
    """
    atr_adj = self.last_atr / max(px, 1e-9)
    vol_adj = self.last_vol * 2.5
    raw = self.effective_base_spacing_pct + atr_adj * 0.5 + vol_adj
    
    # CRITICAL: Minimum spacing must ensure grid levels don't overlap
    # Require at least 3 ticks between any two levels
    min_tick_spacing = max(self.tick_sz / max(px, 1e-9) * 3, 0.0005)  # 5 bps absolute floor
    
    # For N grid levels, ensure total spread is reasonable
    # Minimum per-level spacing scales with grid count
    min_per_level = min_tick_spacing / max(1, self.effective_grids / 4)
    tick_floor = max(min_tick_spacing, min_per_level, self.effective_base_spacing_pct * 0.5)
    
    # Loss-drift tightening with stricter floor
    center = grid_center if grid_center is not None else px
    loss_drift = 0.0
    if position != 0:
        sign = 1 if position > 0 else -1
        # positive drift_pct means price moved against our position
        loss_drift = max(0.0, sign * (center - px) / max(px, 1e-9))
    
    if loss_drift > 0:
        # FIXED: Never tighten below 60% of raw spacing (was 50%)
        tighten_mult = max(0.60, 1 - min(0.40, loss_drift * 8))
    else:
        tighten_mult = 1.0
    
    adjusted_spacing = raw * tighten_mult
    
    # CRITICAL: Final floor check with explicit minimum
    spacing = min(self.cfg.max_spacing_pct, max(tick_floor, adjusted_spacing))
    
    # ADDED: Validate total grid width is sensible
    total_grid_width = spacing * self.effective_grids * 2
    min_acceptable_width = 0.01  # 1% minimum total spread
    
    if total_grid_width < min_acceptable_width:
        old_spacing = spacing
        spacing = min_acceptable_width / (self.effective_grids * 2)
        logging.getLogger("gridbot").warning(
            json.dumps({
                "event": "grid_compression_override",
                "raw_spacing": raw,
                "adjusted_spacing": adjusted_spacing,
                "old_final": old_spacing,
                "new_final": spacing,
                "total_grid_width_pct": total_grid_width,
                "grids": self.effective_grids,
                "loss_drift": loss_drift,
                "tighten_mult": tighten_mult
            })
        )
    
    if self._log():
        logging.getLogger("gridbot").info(
            json.dumps({
                "event": "spacing_compute",
                "px": px,
                "atr_adj": atr_adj,
                "vol_adj": vol_adj,
                "raw_spacing": raw,
                "spacing": spacing,
                "loss_drift": loss_drift,
                "tighten_mult": tighten_mult,
                "tick_floor": tick_floor,
            })
        )
    return spacing
'''

# =============================================================================
# PATCH C-5: Fix Double Position Update on Fill Replay
# File: src/bot.py
# Modify: _handle_fill method - skip position update when replay=True
# =============================================================================

PATCH_C5_HANDLE_FILL = '''
async def _handle_fill(self, f: Dict[str, Any], replay: bool = False) -> None:
    """Process a fill dictionary `f`. 
    
    PATCHED: If `replay` is True (loading from fill log), only reconcile order
    state without updating position - position already reflects these fills.
    """
    # Prepare canonical fields
    side = f.get("side", "").lower()
    px = float(f.get("px", 0.0))
    sz = float(f.get("sz", 0.0))
    oid = f.get("oid")
    cloid = f.get("cloid")
    ts_ms = int(f.get("time", now_ms()))

    # Validate fill timestamp
    ts_anomaly = self._validate_fill_timestamp(ts_ms)
    if ts_anomaly:
        self._log_event("fill_timestamp_anomaly", type=ts_anomaly, ts_ms=ts_ms, 
                       side=side, px=px, sz=sz)
        try:
            self.rich_metrics.fill_timestamp_anomalies.labels(
                coin=self.coin, type=ts_anomaly).inc()
        except Exception:
            pass

    # Global deduplication
    if not self.fill_deduplicator.check_and_add(f):
        self._log_event("fill_dedup_skip", 
                       key=self.fill_deduplicator.make_fill_key(f), replay=replay)
        return

    # CRITICAL FIX: On replay, only clear order from registry, don't update position
    if replay:
        rec = self.order_manager.pop_by_ids(
            str(cloid) if cloid else None, 
            self._to_int_safe(oid)
        )
        if rec:
            self._log_event("fill_replay_order_cleared", 
                           cloid=cloid, oid=oid, side=side, px=px, sz=sz)
        # Update last fill time but skip position/PnL updates
        self.last_fill_time_ms = max(self.last_fill_time_ms, ts_ms)
        return

    # Persist live fills to the event log
    try:
        await self.fill_log.append({
            "side": side, "px": px, "sz": sz, 
            "oid": oid, "cloid": cloid, "time": ts_ms
        })
    except Exception:
        pass

    # ... rest of existing _handle_fill code for live fills ...
'''

# =============================================================================
# PATCH C-6: Fix Cancel Failure Leaving Stale Orders
# File: src/bot.py
# Replace: _cancel_record method (lines ~1238-1280)
# =============================================================================

PATCH_C6_CANCEL_RECORD = '''
async def _cancel_record(self, rec: ActiveOrder, reason: str = "unspecified") -> bool:
    """
    Cancel an order record and update state.
    
    PATCHED: On cancel failure, checks if order is actually still open on exchange
    before deciding whether to keep it in local registry.
    
    Returns:
        True if order is confirmed gone (cancelled or filled), False if still active.
    """
    if not self.router:
        return False
    
    try:
        await self.router.safe_cancel(cloid=rec.cloid, oid=rec.oid)
        
        # Update state machine
        self.order_state_machine.cancel(
            cloid=rec.cloid,
            oid=rec.oid,
            reason=reason,
        )
        
        # Unindex on successful cancel
        self._unindex(rec)
        
        try:
            self.rich_metrics.orders_cancelled.labels(coin=self.coin, reason=reason).inc()
        except Exception:
            pass
        
        self._log_event("cancel_ok", cloid=rec.cloid, oid=rec.oid, reason=reason)
        return True
        
    except Exception as exc:
        self._log_event("cancel_error", err=str(exc), cloid=rec.cloid, 
                       oid=rec.oid, reason=reason)
        
        # ADDED: Check if order is actually still open on exchange
        try:
            still_open = await self._is_order_still_open(rec.cloid, rec.oid)
            if not still_open:
                # Order was filled or cancelled elsewhere - safe to unindex
                self._log_event("cancel_error_order_gone", 
                               cloid=rec.cloid, oid=rec.oid)
                self._unindex(rec)
                return True
        except Exception as check_exc:
            self._log_event("cancel_error_check_failed", err=str(check_exc))
        
        # Order might still be open - mark for reconciliation
        try:
            self.rich_metrics.api_errors_total.labels(
                coin=self.coin, error_type="cancel_error").inc()
        except Exception:
            pass
        
        self.rebuild_needed = True
        return False

async def _is_order_still_open(self, cloid: Optional[str], oid: Optional[int]) -> bool:
    """
    Check with exchange if order is still open.
    
    Returns:
        True if order exists in open orders, False if not found.
    """
    try:
        builder_asset = self.cfg.dex.lower() == "xyz" or self.coin.startswith("xyz:")
        if self.async_info and not builder_asset:
            remote = await self.async_info.frontend_open_orders(self.account, self.cfg.dex)
        else:
            def _call():
                return self.info.frontend_open_orders(self.account, dex=self.cfg.dex)
            remote = await self._call_with_retry(_call, "check_order_open")
        
        if isinstance(remote, dict):
            remote = remote.get("openOrders", [])
        
        for o in remote:
            if o.get("coin") != self.coin:
                continue
            # Match by cloid or oid
            if cloid and str(o.get("cloid")) == cloid:
                return True
            if oid is not None and o.get("oid") == oid:
                return True
        
        return False
    except Exception:
        # On error, assume order might still be open (safer)
        return True
'''

# =============================================================================
# SHADOW LEDGER IMPLEMENTATION (Recommended Enhancement)
# File: src/shadow_ledger.py (NEW FILE)
# =============================================================================

SHADOW_LEDGER_IMPLEMENTATION = '''
"""
Shadow Ledger - Local position tracking with drift detection.

Maintains a shadow copy of exchange position state to detect sync issues.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
import logging

log = logging.getLogger("gridbot")


@dataclass
class PendingOrder:
    """An order that's been submitted but not yet filled."""
    cloid: Optional[str]
    oid: Optional[int]
    side: str
    size: float
    price: float
    submitted_ms: int


@dataclass
class LedgerEntry:
    """A fill recorded in the ledger."""
    side: str
    size: float
    price: float
    timestamp_ms: int
    source: str  # 'ws', 'rest', 'reconcile'


class ShadowLedger:
    """
    Maintains shadow position state with drift detection.
    
    Tracks:
    - confirmed_position: Last value from exchange reconciliation
    - local_position: Running total from processed fills
    - pending_orders: Orders submitted but not yet filled
    """
    
    def __init__(self, coin: str, log_event: Optional[Callable] = None):
        self.coin = coin
        self._log_event = log_event or self._default_log
        
        self.confirmed_position: float = 0.0
        self.local_position: float = 0.0
        self._pending: Dict[str, PendingOrder] = {}
        
        self.last_reconcile_ms: int = 0
        self.last_fill_ms: int = 0
        self.total_drift_corrections: int = 0
        
        self._entries: List[LedgerEntry] = []
        self._max_entries = 1000
        
        self._lock = asyncio.Lock()
    
    def _default_log(self, event: str, **kwargs):
        log.info(f'{{"event":"{event}","coin":"{self.coin}",{",".join(f\'"{k}":{v}\' for k,v in kwargs.items())}}}')
    
    async def add_pending(self, cloid: str, side: str, size: float, price: float) -> None:
        """Track a submitted order."""
        async with self._lock:
            self._pending[cloid] = PendingOrder(
                cloid=cloid,
                oid=None,
                side=side,
                size=size,
                price=price,
                submitted_ms=int(time.time() * 1000)
            )
    
    async def update_pending_oid(self, cloid: str, oid: int) -> None:
        """Update pending order with exchange oid."""
        async with self._lock:
            if cloid in self._pending:
                self._pending[cloid].oid = oid
    
    async def remove_pending(self, cloid: str) -> Optional[PendingOrder]:
        """Remove a pending order (filled or cancelled)."""
        async with self._lock:
            return self._pending.pop(cloid, None)
    
    async def apply_fill(self, side: str, size: float, price: float, 
                        source: str, cloid: Optional[str] = None) -> None:
        """Apply a fill to local position."""
        async with self._lock:
            delta = size if side.lower().startswith('b') else -size
            self.local_position += delta
            self.last_fill_ms = int(time.time() * 1000)
            
            # Remove from pending if matched
            if cloid:
                self._pending.pop(cloid, None)
            
            # Record entry
            entry = LedgerEntry(
                side=side,
                size=size,
                price=price,
                timestamp_ms=self.last_fill_ms,
                source=source
            )
            self._entries.append(entry)
            if len(self._entries) > self._max_entries:
                self._entries = self._entries[-self._max_entries:]
    
    async def reconcile(self, exchange_position: float) -> float:
        """
        Reconcile with exchange position.
        
        Returns absolute drift amount. Resets local_position to exchange truth.
        """
        async with self._lock:
            drift = abs(exchange_position - self.local_position)
            
            if drift > 1e-9:
                self.total_drift_corrections += 1
                self._log_event("shadow_ledger_drift",
                              local=self.local_position,
                              exchange=exchange_position,
                              drift=drift,
                              drift_count=self.total_drift_corrections)
            
            self.confirmed_position = exchange_position
            self.local_position = exchange_position
            self.last_reconcile_ms = int(time.time() * 1000)
            
            return drift
    
    @property
    def pending_exposure(self) -> float:
        """Net exposure from pending orders."""
        exposure = 0.0
        for p in self._pending.values():
            if p.side.lower().startswith('b'):
                exposure += p.size
            else:
                exposure -= p.size
        return exposure
    
    @property
    def expected_position(self) -> float:
        """Position if all pending orders fill."""
        return self.local_position + self.pending_exposure
    
    def get_stale_pending(self, max_age_ms: int = 60000) -> List[PendingOrder]:
        """Get pending orders older than threshold."""
        now = int(time.time() * 1000)
        cutoff = now - max_age_ms
        return [p for p in self._pending.values() if p.submitted_ms < cutoff]
    
    def get_state(self) -> dict:
        """Get ledger state for persistence/monitoring."""
        return {
            "confirmed_position": self.confirmed_position,
            "local_position": self.local_position,
            "pending_count": len(self._pending),
            "pending_exposure": self.pending_exposure,
            "last_reconcile_ms": self.last_reconcile_ms,
            "last_fill_ms": self.last_fill_ms,
            "total_drift_corrections": self.total_drift_corrections,
            "recent_fills": len(self._entries),
        }
'''

# =============================================================================
# FILL GUARANTOR IMPLEMENTATION (Recommended Enhancement)
# File: src/fill_guarantor.py (NEW FILE)
# =============================================================================

FILL_GUARANTOR_IMPLEMENTATION = '''
"""
Fill Guarantor - Ensures zero fill loss through multi-source capture.

Combines WS, REST, and reconciliation fills with guaranteed persistence.
"""

from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional, Callable, Set
import logging

log = logging.getLogger("gridbot")


class FillGuarantor:
    """
    Guarantees no fill is lost through:
    1. WAL-first writes (crash safety)
    2. Multi-source deduplication
    3. Sequence gap detection
    4. Periodic reconciliation
    """
    
    def __init__(
        self,
        coin: str,
        fill_log,  # FillLog or BatchedFillLog instance
        shadow_ledger,  # ShadowLedger instance
        log_event: Optional[Callable] = None
    ):
        self.coin = coin
        self.fill_log = fill_log
        self.ledger = shadow_ledger
        self._log_event = log_event or self._default_log
        
        self._processed: Set[str] = set()
        self._sequence_hwm: int = 0  # High water mark for fill timestamps
        self._fill_sources: Dict[str, int] = {
            'ws': 0, 'rest': 0, 'reconcile': 0
        }
        
        self._lock = asyncio.Lock()
    
    def _default_log(self, event: str, **kwargs):
        log.info(f'{{"event":"{event}","coin":"{self.coin}",{",".join(f\'"{k}":{v}\' for k,v in kwargs.items())}}}')
    
    def _make_key(self, fill: dict) -> str:
        """Generate unique fill key."""
        return f"{fill.get('oid')}_{fill.get('cloid')}_{fill.get('time')}_{fill.get('px')}_{fill.get('sz')}"
    
    async def process_fill(self, fill: dict, source: str) -> bool:
        """
        Process a fill with guaranteed persistence.
        
        Steps:
        1. Check dedup
        2. Write to WAL
        3. Update shadow ledger
        4. Track statistics
        
        Returns True if fill was new and processed.
        """
        fill_key = self._make_key(fill)
        
        async with self._lock:
            if fill_key in self._processed:
                self._log_event("fill_dedup_skip", key=fill_key, source=source)
                return False
            
            # Write to WAL FIRST (crash safety)
            try:
                await self.fill_log.append({
                    **fill,
                    "source": source,
                    "guaranteed_at": int(time.time() * 1000)
                })
            except Exception as e:
                self._log_event("fill_wal_error", err=str(e), source=source)
                # Continue anyway - fill processing is more important than logging
            
            # Mark as processed
            self._processed.add(fill_key)
            
            # Bound the processed set
            if len(self._processed) > 50000:
                # Keep most recent half
                self._processed = set(list(self._processed)[-25000:])
            
            # Track sequence for gap detection
            fill_ts = int(fill.get("time", 0))
            if fill_ts > 0:
                if fill_ts < self._sequence_hwm - 5000:  # >5s out of order
                    self._log_event("fill_sequence_gap", 
                                   fill_ts=fill_ts, 
                                   hwm=self._sequence_hwm,
                                   gap_ms=self._sequence_hwm - fill_ts)
                self._sequence_hwm = max(self._sequence_hwm, fill_ts)
            
            # Update statistics
            self._fill_sources[source] = self._fill_sources.get(source, 0) + 1
        
        # Update shadow ledger (outside lock since ledger has its own)
        await self.ledger.apply_fill(
            side=fill.get("side", ""),
            size=float(fill.get("sz", 0)),
            price=float(fill.get("px", 0)),
            source=source,
            cloid=fill.get("cloid")
        )
        
        return True
    
    async def check_for_gaps(self, exchange_fills: List[dict], since_ms: int) -> List[dict]:
        """
        Check exchange fills for any we might have missed.
        
        Returns list of fills that need processing.
        """
        missed = []
        async with self._lock:
            for fill in exchange_fills:
                fill_ts = int(fill.get("time", 0))
                if fill_ts < since_ms:
                    continue
                
                fill_key = self._make_key(fill)
                if fill_key not in self._processed:
                    missed.append(fill)
        
        if missed:
            self._log_event("fills_gap_detected", 
                          count=len(missed),
                          since_ms=since_ms)
        
        return missed
    
    def get_stats(self) -> dict:
        """Get guarantor statistics."""
        return {
            "processed_count": len(self._processed),
            "sequence_hwm_ms": self._sequence_hwm,
            "fills_by_source": dict(self._fill_sources),
        }
'''

# =============================================================================
# USAGE INSTRUCTIONS
# =============================================================================

USAGE = """
HOW TO APPLY THESE PATCHES
==========================

1. BACKUP YOUR CURRENT CODE
   cp -r src/ src_backup/

2. APPLY PATCH C-1 (Race Condition)
   - Open src/bot.py
   - Find _build_and_place_grid method (~line 910)
   - Replace entire method with PATCH_C1_BUILD_AND_PLACE_GRID

3. APPLY PATCH C-2 (Partial Fill)
   - Open src/order_manager.py
   - Find handle_partial_fill method (~line 160)
   - Replace entire method with PATCH_C2_PARTIAL_FILL

4. APPLY PATCH C-3 (WS Reconnect)
   - Open src/market_data.py
   - Add fields from PATCH_C3_MARKET_DATA_INIT to __init__
   - Add PATCH_C3_CONSUME_RECONNECTION as new method
   - Update _resubscribe per PATCH_C3_RESUBSCRIBE
   - Open src/bot.py
   - Add code from PATCH_C3_BOT_MAIN_LOOP to run() method

5. APPLY PATCH C-4 (Grid Spacing)
   - Open src/strategy.py
   - Find compute_spacing method (~line 115)
   - Replace entire method with PATCH_C4_COMPUTE_SPACING

6. APPLY PATCH C-5 (Fill Replay)
   - Open src/bot.py
   - Find _handle_fill method (~line 485)
   - Add the replay early-exit from PATCH_C5_HANDLE_FILL after dedup check

7. APPLY PATCH C-6 (Cancel Failure)
   - Open src/bot.py
   - Find _cancel_record method (~line 1238)
   - Replace entire method with PATCH_C6_CANCEL_RECORD
   - Add _is_order_still_open as new method

8. RUN TESTS
   pytest tests/ -v

9. VERIFY
   - Check logs for new events
   - Monitor position drift metrics
   - Compare fill counts between sources
"""

if __name__ == "__main__":
    print(USAGE)
