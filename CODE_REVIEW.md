# GridBot Comprehensive Code Review
## Institutional-Grade Reliability Analysis

**Review Date:** December 10, 2025  
**Target:** 99.99% reliability, zero missed fills

---

# TABLE OF CONTENTS

1. [Executive Summary](#executive-summary)
2. [Critical Findings (P0)](#critical-findings-p0)
3. [Major Issues (P1)](#major-issues-p1)
4. [Structural Improvements (P2)](#structural-improvements-p2)
5. [Reliability Hardening](#reliability-hardening)
6. [Architecture Blueprint](#architecture-blueprint)
7. [Prioritized Fix List](#prioritized-fix-list)
8. [Pre-Live Checklist](#pre-live-checklist)

---

# EXECUTIVE SUMMARY

## Overall Assessment: **7/10 - Solid Foundation, Needs Hardening**

### Strengths
- Well-structured modular design (OrderManager, PositionTracker, GridCalculator, StateMachine)
- Good async/await patterns with proper lock usage
- Comprehensive metrics and logging infrastructure
- Smart fill deduplication and circuit breaker patterns
- Batched write optimization for fill logs

### Critical Gaps
1. **Race Condition in Fill Processing** - Position updates not atomic with order state
2. **Incomplete Shadow Ledger** - Local state can drift from exchange
3. **Partial Fill Handling Gaps** - 0.99 threshold can miss edge cases
4. **Grid Compression Vulnerability** - Can collapse during extreme volatility
5. **WS Fill Loss During Reconnect** - Gap between disconnect and REST poll

---

# CRITICAL FINDINGS (P0)

## C-1: Race Condition Between Fill Processing and Grid Rebuild

### Root Cause
In `_handle_fill()`, position is updated under `position_lock`, but `_build_and_place_grid()` takes a snapshot at a different time. Between snapshot and order placement, fills can arrive causing stale grid construction.

```python
# bot.py:919-920 - Gap between snapshot and order placement
async with self.position_lock:
    position_snapshot = self.position  # Snapshot here

# ... grid computation happens (no lock) ...
# ... fills can arrive and change self.position ...

await self._submit_levels_batch(to_place, mode="grid")  # Placed with stale position
```

### Detection
Monitor `grid_position_drift` events in logs. If `drift_pct > 0.1` occurs frequently, this race is active.

### Fix
```python
# PATCH: bot.py - Atomic grid build with position lock held longer

async def _build_and_place_grid(self, mid: float) -> None:
    if not self.strategy or not self.router:
        return
    if mid <= 0:
        self._log_event("grid_build_skip_mid", mid=mid)
        return
    
    # CRITICAL: Hold lock through entire grid computation AND diff check
    async with self.position_lock:
        position_snapshot = self.position
        
        # Update strategy internal models
        try:
            self.strategy.on_price(mid)
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
        
        levels = build_result.levels
        spacing = build_result.spacing
        
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
    
    # Release lock before I/O operations (cancel/place)
    # Note: Short window here is acceptable since we've computed everything
    
    for key in to_cancel_keys:
        rec = self.orders_by_price.get(key)
        if rec:
            try:
                await self._cancel_record(rec, reason="grid_diff")
            except Exception as exc:
                self._log_event("cancel_diff_error", err=str(exc), key=key)
    
    # Final position check before placement
    async with self.position_lock:
        current_pos = self.position
    
    if abs(current_pos - position_snapshot) / max(abs(position_snapshot), 1e-9) > 0.1:
        self._log_event("grid_rebuild_aborted_drift", 
                       snapshot=position_snapshot, 
                       current=current_pos)
        self.rebuild_needed = True
        return
    
    await self._submit_levels_batch(to_place, mode="grid")
```

---

## C-2: Partial Fill Threshold Bug (0.99 can lose fills)

### Root Cause
In `OrderManager.handle_partial_fill()`, orders are only unindexed when `filled_qty >= original_qty * 0.99`. This 1% tolerance can cause:
1. Very small final fills to be missed
2. Order to remain in registry after exchange considers it filled

```python
# order_manager.py:176-178
if rec.original_qty > 0 and rec.filled_qty >= rec.original_qty * 0.99:
    self._unindex(rec)  # Only unindex if 99%+ filled
```

### Detection
Search logs for orders with `filled_qty` between 99% and 100% that never get unindexed.

### Fix
```python
# PATCH: order_manager.py - Use absolute tolerance, not percentage

def handle_partial_fill(self, cloid: Optional[str], oid: Optional[int], 
                       fill_sz: float, min_remaining_sz: float = 0.001) -> Optional[ActiveOrder]:
    """
    Handle partial fill - update filled_qty, only remove if fully filled.
    
    Args:
        min_remaining_sz: Minimum remaining size to consider order still active.
                         Orders with remaining < this are considered filled.
    
    Returns the ActiveOrder if found (for position updates), None otherwise.
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
    
    # FIXED: Use absolute threshold, not percentage
    # Order is "filled" if remaining is below minimum tradeable size
    is_fully_filled = remaining < min_remaining_sz
    
    if is_fully_filled:
        self._unindex(rec)
        log.info(f'{{"event":"order_filled_complete","cloid":"{cloid}","oid":{oid},'
                 f'"filled_qty":{rec.filled_qty},"original_qty":{rec.original_qty}}}')
        return rec
    
    # Partial fill: keep order in registry
    log.info(f'{{"event":"partial_fill","cloid":"{cloid}","oid":{oid},'
             f'"fill_sz":{fill_sz},"filled_qty":{rec.filled_qty},'
             f'"remaining":{remaining},"original_qty":{rec.original_qty}}}')
    return rec
```

---

## C-3: WS Fill Loss During Reconnection Window

### Root Cause
When WebSocket disconnects and reconnects, there's a gap where fills can be missed:
1. WS disconnects at T1
2. Bot detects staleness at T1 + `ws_stale_after` (default 20s)
3. REST poll happens at T1 + N (based on `rest_fill_interval`)
4. Fills between T1 and first REST poll are lost

The `min_fill_time_ms` filter in `_on_user_fills` also drops legitimate fills from the reconnection snapshot.

### Detection
Compare `fills_total` counter vs exchange fill history. Discrepancies indicate missed fills.

### Fix
```python
# PATCH: market_data.py - Track reconnection and force immediate REST poll

class MarketData:
    def __init__(self, ...):
        # ... existing init ...
        self._reconnection_pending = False
        self._last_ws_disconnect_ms = 0
    
    def _resubscribe(self) -> None:
        """Mark reconnection pending for bot to trigger REST poll."""
        self._reconnection_pending = True
        self._last_ws_disconnect_ms = int(time.time() * 1000)
        # ... existing resubscribe logic ...
    
    def consume_reconnection_event(self) -> Optional[int]:
        """
        Check if reconnection occurred and return disconnect timestamp.
        Returns None if no reconnection pending.
        Bot should poll REST fills from returned timestamp.
        """
        if self._reconnection_pending:
            self._reconnection_pending = False
            return self._last_ws_disconnect_ms
        return None

# PATCH: bot.py - Poll REST immediately after reconnection

async def run(self) -> None:
    # ... in main loop ...
    
    # Check for WS reconnection and poll missed fills
    reconnect_ts = self.market.consume_reconnection_event()
    if reconnect_ts:
        self._log_event("ws_reconnect_fill_poll", since_ms=reconnect_ts)
        try:
            fills = await self.market.user_fills_since(
                self.account, 
                max(0, reconnect_ts - 5000)  # 5s buffer
            )
            for f in sorted(fills, key=lambda x: int(x.get("time", 0))):
                if f.get("coin") != self.coin:
                    continue
                await self._handle_fill(f)
        except Exception as exc:
            self._log_event("ws_reconnect_fill_poll_error", err=str(exc))
```

---

## C-4: Grid Spacing Can Collapse to Zero

### Root Cause
In `GridStrategy.compute_spacing()`, the loss-drift tightening can reduce spacing to `min_spread_spacing`, but if multiple conditions stack:
1. High loss_drift → `tighten_mult = 0.50`
2. Low base spacing from ATR → compounds reduction
3. Result: Grid levels can become so close they overlap

```python
# strategy.py:136-138 - Tightening can stack with low ATR
if loss_drift > 0:
    tighten_mult = max(0.50, 1 - min(0.50, loss_drift * 10))
```

### Detection
Log events `spacing_compression_prevented` indicate this protection triggered. If grid levels are within 2 ticks of each other, compression occurred.

### Fix
```python
# PATCH: strategy.py - Enforce minimum grid separation

def compute_spacing(self, px: float, position: float = 0.0, 
                   grid_center: Optional[float] = None) -> float:
    atr_adj = self.last_atr / max(px, 1e-9)
    vol_adj = self.last_vol * 2.5
    raw = self.effective_base_spacing_pct + atr_adj * 0.5 + vol_adj
    
    # CRITICAL: Minimum spacing must ensure grid levels don't overlap
    # Require at least 3 ticks between any two levels
    min_tick_spacing = max(self.tick_sz / max(px, 1e-9) * 3, 0.0005)  # 5 bps floor
    
    # For N grid levels, total spread = 2 * N * spacing
    # Minimum per-level spacing = min_spread / N
    min_per_level = min_tick_spacing / max(1, self.effective_grids / 4)
    tick_floor = max(min_tick_spacing, min_per_level, self.effective_base_spacing_pct * 0.5)
    
    # Apply loss-drift tightening with stricter floor
    center = grid_center if grid_center is not None else px
    loss_drift = 0.0
    if position != 0:
        sign = 1 if position > 0 else -1
        loss_drift = max(0.0, sign * (center - px) / max(px, 1e-9))
    
    if loss_drift > 0:
        # FIXED: Never tighten below 60% of raw spacing
        tighten_mult = max(0.60, 1 - min(0.40, loss_drift * 8))
    else:
        tighten_mult = 1.0
    
    adjusted_spacing = raw * tighten_mult
    
    # CRITICAL: Final floor check with explicit minimum
    spacing = min(self.cfg.max_spacing_pct, max(tick_floor, adjusted_spacing))
    
    # ADDED: Validate total grid width is sensible
    total_grid_width = spacing * self.effective_grids * 2
    if total_grid_width < 0.01:  # Less than 1% total spread is dangerous
        spacing = 0.01 / (self.effective_grids * 2)
        self._log_compressed_grid(raw, spacing, total_grid_width)
    
    return spacing

def _log_compressed_grid(self, raw: float, final: float, total_width: float) -> None:
    import logging
    logging.getLogger("gridbot").warning(json.dumps({
        "event": "grid_compression_override",
        "raw_spacing": raw,
        "final_spacing": final,
        "total_grid_width_pct": total_width,
        "grids": self.effective_grids
    }))
```

---

## C-5: Double Position Update on Fill Replay

### Root Cause
When fills are replayed from `fill_log.read_since()` on startup, `_handle_fill(f, replay=True)` still updates position even though position was loaded from state.

```python
# bot.py:282-289 - Replay still updates position
events = await self.fill_log.read_since(self.last_fill_time_ms)
for ev in events:
    try:
        await self._handle_fill(ev, replay=True)  # Updates position!
    except Exception:
        pass
```

This causes position drift if fills are in the log but position was already saved reflecting those fills.

### Detection
Position immediately after startup differs from saved state position.

### Fix
```python
# PATCH: bot.py - Replay should NOT update position, only rebuild order indices

async def _handle_fill(self, f: Dict[str, Any], replay: bool = False) -> None:
    """Process a fill dictionary `f`. If `replay` is True, only rebuilds order
    state without updating position (position already reflects these fills)."""
    
    # ... existing dedup and validation ...
    
    if not self.fill_deduplicator.check_and_add(f):
        self._log_event("fill_dedup_skip", key=self.fill_deduplicator.make_fill_key(f), replay=replay)
        return
    
    # CRITICAL: On replay, only reconcile order state, don't update position
    if replay:
        # Just unindex the order if it exists
        rec = self.order_manager.pop_by_ids(str(cloid) if cloid else None, self._to_int_safe(oid))
        if rec:
            self._log_event("fill_replay_order_cleared", cloid=cloid, oid=oid)
        return
    
    # ... rest of fill handling for live fills ...
```

---

## C-6: Cancel Failure Leaves Order in Registry

### Root Cause
When `_cancel_record()` fails, the order remains in `orders_by_price` even though the exchange may have already filled it.

```python
# bot.py:1247-1268 - Cancel error path
except Exception as exc:
    # ... logging ...
    self.rebuild_needed = True
    return False

# Critical-8: Only unindex on confirmed cancel
if success:
    self._unindex(rec)  # Never called if exception
```

### Detection
Orders stuck in local registry that don't exist on exchange (visible in `_reconcile_orders` adding/removing).

### Fix
```python
# PATCH: bot.py - On cancel failure, check order status before deciding

async def _cancel_record(self, rec: ActiveOrder, reason: str = "unspecified") -> bool:
    """Return True if order is confirmed gone, False if still active."""
    if not self.router:
        return False
    
    try:
        await self.router.safe_cancel(cloid=rec.cloid, oid=rec.oid)
        self.order_state_machine.cancel(cloid=rec.cloid, oid=rec.oid, reason=reason)
        self._unindex(rec)
        return True
    except Exception as exc:
        self._log_event("cancel_error", err=str(exc), cloid=rec.cloid, oid=rec.oid)
        
        # ADDED: Check if order is actually still open
        try:
            still_open = await self._is_order_still_open(rec.cloid, rec.oid)
            if not still_open:
                # Order was filled or cancelled elsewhere - unindex it
                self._log_event("cancel_error_order_gone", cloid=rec.cloid, oid=rec.oid)
                self._unindex(rec)
                return True
        except Exception:
            pass
        
        self.rebuild_needed = True
        return False

async def _is_order_still_open(self, cloid: Optional[str], oid: Optional[int]) -> bool:
    """Check exchange if order is still open."""
    try:
        remote = await self.async_info.frontend_open_orders(self.account, self.cfg.dex)
        if isinstance(remote, dict):
            remote = remote.get("openOrders", [])
        for o in remote:
            if o.get("coin") != self.coin:
                continue
            if cloid and str(o.get("cloid")) == cloid:
                return True
            if oid is not None and o.get("oid") == oid:
                return True
        return False
    except Exception:
        return True  # Assume open on error (safer)
```

---

# MAJOR ISSUES (P1)

## M-1: No Sequence Guarantee for Multi-Fill Events

### Problem
When a fast wick fills multiple orders simultaneously, fills arrive out-of-order. The current dedup uses a flat set without sequence tracking.

### Fix
```python
# PATCH: fill_deduplicator.py - Add sequence tracking

class FillDeduplicator:
    def __init__(self, ...):
        # ... existing init ...
        self._sequence: Dict[str, int] = {}  # coin -> expected next sequence
        
    def check_and_add_sequenced(self, fill: Dict[str, Any], coin: str) -> Tuple[bool, bool]:
        """
        Check fill and track sequence.
        
        Returns:
            (is_new, is_gap) - is_new=True if should process, is_gap=True if sequence gap
        """
        fill_key = self.make_fill_key(fill)
        ts = int(fill.get("time", 0))
        
        if fill_key in self._processed:
            return False, False
        
        # Check for sequence gap (fills arriving out of order by > 1 second)
        last_seq = self._sequence.get(coin, 0)
        is_gap = ts > 0 and last_seq > 0 and ts < last_seq - 1000
        
        self._add_key(fill_key)
        self._sequence[coin] = max(self._sequence.get(coin, 0), ts)
        
        return True, is_gap
```

---

## M-2: REST Fill Poll Can Miss Fills During Grid Rebuild

### Problem
`_poll_rest_fills()` uses `start_ms = max(last_fill_time - rescan, last_poll - buffer)`. If a fill occurs during grid rebuild and `last_fill_time_ms` updates, the next poll might skip fills in between.

### Fix
```python
# PATCH: bot.py - Track high watermark separately from last processed

def __init__(self, ...):
    # ... existing init ...
    self._rest_poll_hwm_ms = 0  # High water mark for REST polling

async def _poll_rest_fills(self, force: bool = False) -> None:
    # Use dedicated HWM, not last_fill_time which can jump
    start_ms = max(0, self._rest_poll_hwm_ms - self.cfg.fill_rescan_ms)
    
    fills = await self.market.user_fills_since(self.account, start_ms)
    max_ts = start_ms
    
    for f in sorted(fills, key=lambda x: int(x.get("time", 0))):
        if f.get("coin") != self.coin:
            continue
        fill_ts = int(f.get("time", 0))
        max_ts = max(max_ts, fill_ts)
        await self._handle_fill(f)
    
    # Update HWM to highest seen timestamp
    self._rest_poll_hwm_ms = max(self._rest_poll_hwm_ms, max_ts)
```

---

## M-3: Order Router Coalescing Can Cause Stale Orders

### Problem
Adaptive coalescing based on latency percentiles can delay orders during volatility, causing them to be placed at stale prices.

```python
# order_router.py:220-227 - Coalescing based on latency
target = int(min(self.coalesce_max_ms, max(self.coalesce_min_ms, pct_val * 0.5)))
self.coalesce_ms = target
```

During fast markets, 50-200ms delay is significant.

### Fix
```python
# PATCH: order_router.py - Add volatility-aware coalescing

def _adjust_coalesce_for_volatility(self, volatility: float) -> None:
    """Reduce coalescing during high volatility."""
    if volatility > 0.02:  # >2% EWMA vol
        # During volatility, minimize delay
        self.coalesce_ms = self.coalesce_min_ms
    elif volatility > 0.01:
        # Moderate volatility - halve coalesce time
        self.coalesce_ms = max(self.coalesce_min_ms, self.coalesce_ms // 2)

# Call from bot when volatility changes
async def _build_and_place_grid(self, mid: float) -> None:
    # ... after strategy.on_price(mid) ...
    if self.router and self.strategy:
        self.router._adjust_coalesce_for_volatility(self.strategy.last_vol)
```

---

## M-4: Risk Engine Position Check Uses Stale Position

### Problem
`RiskEngine.allow_order()` uses `self.state.position` which is updated asynchronously. During rapid fills, the risk check may use stale position data.

### Fix
```python
# PATCH: risk.py - Add position override and real-time check

def allow_order(
    self,
    side: str,
    sz: float,
    px: float,
    position_override: Optional[float] = None,  # Allow caller to provide fresh position
    record_if_allowed: bool = True,
) -> bool:
    """
    Args:
        position_override: If provided, use this instead of internal state.
                          Caller should provide current position for accuracy.
    """
    with self._lock:
        base_pos = position_override if position_override is not None else self.state.position
    # ... rest of checks using base_pos ...
```

---

## M-5: State Persistence Not Crash-Safe

### Problem
State is persisted in `_persist()` after fills, but if process crashes between fill processing and persist, state is lost.

### Fix
```python
# PATCH: bot.py - Write-ahead logging for critical state

async def _handle_fill(self, f: Dict[str, Any], replay: bool = False) -> None:
    # ... after position update, before anything else ...
    
    # CRITICAL: WAL entry before any other processing
    wal_entry = {
        "type": "fill",
        "ts_ms": now_ms(),
        "side": side,
        "px": px,
        "sz": sz,
        "position_after": self.position,
        "pnl_after": self._session_realized_pnl,
    }
    await self._write_wal(wal_entry)
    
    # ... rest of fill handling ...

async def _write_wal(self, entry: Dict[str, Any]) -> None:
    """Write-ahead log for crash recovery."""
    # Use fill_log infrastructure with sync flush
    await self.fill_log.append(entry)
    if hasattr(self.fill_log, '_flush'):
        await self.fill_log._flush()  # Force immediate write
```

---

## M-6: Floating-Point Drift in Price Keys

### Problem
`price_key()` uses string formatting which can produce different strings for mathematically equal floats:
```python
# Example: 100.0000000001 vs 100.0 produce different keys
```

### Current Mitigation
The code uses `Decimal` in several places, but not consistently.

### Fix
```python
# PATCH: order_manager.py - Canonical price key with Decimal

from decimal import Decimal, ROUND_HALF_UP

def price_key(self, lvl: GridLevel) -> str:
    """Generate canonical price key immune to float drift."""
    # First quantize to tick
    px_val = quantize(lvl.px, self.tick_sz, lvl.side, self.px_decimals)
    
    # Convert to Decimal with explicit precision
    px_dec = Decimal(str(px_val))
    
    # Round to tick precision
    if self.tick_sz > 0:
        tick_dec = Decimal(str(self.tick_sz))
        px_dec = (px_dec / tick_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_dec
    
    # Format with fixed decimals
    px_str = format(px_dec, f'.{self.px_decimals}f')
    return f"{lvl.side}:{px_str}"
```

---

# STRUCTURAL IMPROVEMENTS (P2)

## S-1: Implement Shadow Ledger Pattern

### Current State
Position is tracked locally but relies on periodic reconciliation with exchange.

### Recommended Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                      SHADOW LEDGER                          │
├─────────────────────────────────────────────────────────────┤
│  Local Position  │  Exchange Position  │  Pending Fills    │
│     (real-time)  │   (periodic sync)   │  (in-flight)      │
├─────────────────────────────────────────────────────────────┤
│           position = local + pending_buys - pending_sells   │
└─────────────────────────────────────────────────────────────┘
```

### Implementation
```python
# NEW FILE: src/shadow_ledger.py

from dataclasses import dataclass, field
from typing import Dict, Optional
import asyncio
import time


@dataclass
class PendingFill:
    """Represents an expected fill from a resting order."""
    cloid: Optional[str]
    oid: Optional[int]
    side: str
    size: float
    price: float
    created_ms: int


class ShadowLedger:
    """
    Maintains local position shadow with in-flight order tracking.
    
    Position states:
    - confirmed_position: Last reconciled with exchange
    - pending_position: Expected position including in-flight orders
    - local_position: Real-time local tracking from fills
    """
    
    def __init__(self, coin: str, log_event=None):
        self.coin = coin
        self._log = log_event or (lambda e, **k: None)
        
        # Positions
        self.confirmed_position: float = 0.0  # From exchange
        self.local_position: float = 0.0      # From local fills
        self._pending: Dict[str, PendingFill] = {}  # Resting orders
        
        # Timestamps
        self.last_reconcile_ms: int = 0
        self.last_fill_ms: int = 0
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
    
    async def add_pending_order(self, cloid: str, side: str, size: float, price: float) -> None:
        """Track a submitted order as pending."""
        async with self._lock:
            self._pending[cloid] = PendingFill(
                cloid=cloid,
                oid=None,
                side=side,
                size=size,
                price=price,
                created_ms=int(time.time() * 1000)
            )
    
    async def remove_pending_order(self, cloid: str) -> Optional[PendingFill]:
        """Remove a pending order (filled or cancelled)."""
        async with self._lock:
            return self._pending.pop(cloid, None)
    
    async def apply_fill(self, side: str, size: float, cloid: Optional[str] = None) -> None:
        """Apply a fill to local position."""
        async with self._lock:
            if side.lower().startswith('b'):
                self.local_position += size
            else:
                self.local_position -= size
            
            if cloid:
                self._pending.pop(cloid, None)
            
            self.last_fill_ms = int(time.time() * 1000)
    
    async def reconcile(self, exchange_position: float) -> float:
        """
        Reconcile with exchange position.
        
        Returns drift amount (exchange - local).
        """
        async with self._lock:
            drift = exchange_position - self.local_position
            
            if abs(drift) > 1e-9:
                self._log("shadow_ledger_drift",
                         coin=self.coin,
                         local=self.local_position,
                         exchange=exchange_position,
                         drift=drift)
            
            self.confirmed_position = exchange_position
            self.local_position = exchange_position  # Reset to truth
            self.last_reconcile_ms = int(time.time() * 1000)
            
            return drift
    
    @property
    def pending_exposure(self) -> float:
        """Total pending exposure from resting orders."""
        exposure = 0.0
        for p in self._pending.values():
            if p.side == 'buy':
                exposure += p.size
            else:
                exposure -= p.size
        return exposure
    
    @property
    def effective_position(self) -> float:
        """Position including pending orders."""
        return self.local_position + self.pending_exposure
    
    def get_drift_age_ms(self) -> int:
        """Time since last reconciliation."""
        if self.last_reconcile_ms == 0:
            return 0
        return int(time.time() * 1000) - self.last_reconcile_ms
```

---

## S-2: Event-Driven Architecture Refactor

### Current State
Main loop polls at fixed intervals. Fill processing is reactive but not fully event-driven.

### Recommended Architecture
```
┌───────────────────────────────────────────────────────────────────┐
│                        EVENT BUS                                   │
├───────────────────────────────────────────────────────────────────┤
│  Events:                                                          │
│  - FillEvent(side, px, sz, oid, cloid, ts)                       │
│  - OrderAckEvent(oid, cloid, status)                             │
│  - PriceUpdateEvent(mid, bid, ask, ts)                           │
│  - PositionChangeEvent(old_pos, new_pos)                         │
│  - RiskAlertEvent(type, details)                                  │
│  - GridDriftEvent(anchor, mid, drift_pct)                        │
└───────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────┐      ┌──────────────┐
│ FillHandler  │    │ GridManager  │      │ RiskMonitor  │
│ - update pos │    │ - rebuild    │      │ - check limits│
│ - log fill   │    │ - replace    │      │ - halt if bad │
│ - persist    │    │ - rebalance  │      │ - alert       │
└──────────────┘    └──────────────┘      └──────────────┘
```

### Implementation Sketch
```python
# NEW FILE: src/event_bus.py

from dataclasses import dataclass
from typing import Callable, Dict, List, Any
from enum import Enum, auto
import asyncio


class EventType(Enum):
    FILL = auto()
    ORDER_ACK = auto()
    ORDER_REJECT = auto()
    ORDER_CANCEL = auto()
    PRICE_UPDATE = auto()
    POSITION_CHANGE = auto()
    RISK_ALERT = auto()
    GRID_DRIFT = auto()
    WS_DISCONNECT = auto()
    WS_RECONNECT = auto()


@dataclass
class Event:
    type: EventType
    data: Dict[str, Any]
    timestamp_ms: int


class EventBus:
    """Central event bus for decoupled component communication."""
    
    def __init__(self):
        self._subscribers: Dict[EventType, List[Callable]] = {}
        self._queue: asyncio.Queue = asyncio.Queue()
        self._running = False
    
    def subscribe(self, event_type: EventType, handler: Callable) -> None:
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)
    
    async def publish(self, event: Event) -> None:
        await self._queue.put(event)
    
    async def start(self) -> None:
        self._running = True
        while self._running:
            event = await self._queue.get()
            handlers = self._subscribers.get(event.type, [])
            for handler in handlers:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(event)
                    else:
                        handler(event)
                except Exception as e:
                    # Log but don't stop processing
                    pass
    
    def stop(self) -> None:
        self._running = False
```

---

## S-3: Improve Order State Machine Integration

### Current Issue
Order state machine exists but isn't fully integrated with order flow. Orders can exist in OrderManager but not in StateMachine.

### Fix
```python
# PATCH: bot.py - Ensure state machine is always in sync

def _register_order(self, lvl: GridLevel, cloid: Optional[str], oid: Optional[int]) -> ActiveOrder:
    """Register order with both manager and state machine."""
    rec = self.order_manager.register(lvl, cloid, oid)
    
    # Ensure state machine has this order
    if cloid and not self.order_state_machine.get_order(cloid=cloid):
        self.order_state_machine.create_order(
            cloid=cloid,
            oid=oid,
            side=lvl.side,
            price=lvl.px,
            qty=lvl.sz,
        )
    
    return rec

def _unindex(self, rec: ActiveOrder) -> None:
    """Remove from both manager and state machine."""
    self.order_manager.unindex(rec)
    self.order_state_machine.remove_order(cloid=rec.cloid, oid=rec.oid)
```

---

# RELIABILITY HARDENING

## H-1: Guaranteed Fill Capture Architecture

### Current Flow
```
WS Fill Event ─────────┬───────────> Fill Queue ──────> Fill Handler
                       │
REST Poll ─────────────┘
```

### Recommended Flow
```
                   ┌─────────────────────────────────────────────┐
                   │              FILL GUARANTOR                  │
                   ├─────────────────────────────────────────────┤
WS Fill ──────────>│                                             │
                   │  1. Write to WAL immediately                │
REST Poll ────────>│  2. Dedup against processed set            │
                   │  3. Update shadow ledger                    │
Position Update ──>│  4. Reconcile with exchange periodically   │
                   │  5. Alert on drift > threshold              │
                   └─────────────────────────────────────────────┘
```

### Implementation
```python
# NEW FILE: src/fill_guarantor.py

class FillGuarantor:
    """
    Ensures zero fill loss through multi-source capture and reconciliation.
    """
    
    def __init__(self, coin: str, fill_log: FillLog, shadow_ledger: ShadowLedger):
        self.coin = coin
        self.fill_log = fill_log
        self.ledger = shadow_ledger
        self._processed: set = set()
        self._pending_reconcile: List[Dict] = []
    
    async def process_fill(self, fill: Dict, source: str) -> bool:
        """
        Process fill from any source (WS, REST, position reconcile).
        
        Returns True if fill was new and processed, False if duplicate.
        """
        fill_key = self._make_key(fill)
        
        # 1. Check if already processed
        if fill_key in self._processed:
            return False
        
        # 2. Write to WAL FIRST (crash safety)
        await self.fill_log.append({
            **fill,
            "source": source,
            "processed_at": int(time.time() * 1000)
        })
        
        # 3. Mark as processed
        self._processed.add(fill_key)
        
        # 4. Update shadow ledger
        await self.ledger.apply_fill(
            side=fill.get("side", ""),
            size=float(fill.get("sz", 0)),
            cloid=fill.get("cloid")
        )
        
        return True
    
    async def reconcile_with_exchange(self, exchange_fills: List[Dict]) -> List[Dict]:
        """
        Compare exchange fills with processed set.
        Returns list of missed fills that need processing.
        """
        missed = []
        for fill in exchange_fills:
            fill_key = self._make_key(fill)
            if fill_key not in self._processed:
                missed.append(fill)
        return missed
    
    def _make_key(self, fill: Dict) -> str:
        return f"{fill.get('oid')}_{fill.get('time')}_{fill.get('px')}_{fill.get('sz')}"
```

---

## H-2: Reconciliation Loop Best Practices

### Current Issue
Reconciliation happens on fixed interval (`rest_audit_interval`), regardless of system state.

### Recommended Pattern
```python
# PATCH: bot.py - Adaptive reconciliation

async def _should_reconcile(self) -> bool:
    """Determine if reconciliation is needed now."""
    now = time.time()
    
    # Always reconcile after these conditions:
    # 1. WS was recently disconnected
    if self.market and self.market.data_age() > self.cfg.ws_stale_after:
        return True
    
    # 2. High fill activity (potential for drift)
    recent_fills = self.fill_deduplicator._stats.get("processed", 0)
    if recent_fills > 10 and now - self.last_resync > 30:  # 30s during high activity
        return True
    
    # 3. Position drift detected previously
    if hasattr(self, '_last_drift_pct') and self._last_drift_pct > 0.05:
        return True
    
    # 4. Regular interval
    if now - self.last_resync > self.cfg.rest_audit_interval:
        return True
    
    return False

async def run(self) -> None:
    # ... in main loop ...
    
    # Adaptive reconciliation
    if await self._should_reconcile():
        try:
            drift = await self._reconcile_position()
            self._last_drift_pct = drift
            self._reset_api_errors()
        except Exception as exc:
            await self._handle_api_error("reconcile_position", exc)
        self.last_resync = time.time()
```

---

## H-3: Circuit Breaker Improvements

### Current Issue
Circuit breaker trips on error count, but doesn't consider error types or recovery conditions.

### Fix
```python
# PATCH: circuit_breaker.py - Smarter tripping

class CircuitBreaker:
    def record_error(self, where: str, error: Exception) -> bool:
        """Record error with type-aware handling."""
        
        # Classify error severity
        error_str = str(error).lower()
        
        # Critical errors trip immediately
        if any(x in error_str for x in ['rate limit', '429', 'banned', 'suspended']):
            self._cooldown_until = time.time() + 60  # 60s for rate limits
            return self._trip(where)
        
        # Network errors get higher threshold
        if any(x in error_str for x in ['timeout', 'connection', 'network']):
            threshold = self.config.error_threshold * 2
        else:
            threshold = self.config.error_threshold
        
        self.error_streak += 1
        
        if self.error_streak >= threshold:
            return self._trip(where)
        
        return False
```

---

# ARCHITECTURE BLUEPRINT

## System Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              GRIDBOT ARCHITECTURE                           │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────┐     ┌───────────────┐     ┌───────────────┐            │
│  │   WEBSOCKET   │     │   REST API    │     │   EXCHANGE    │            │
│  │    FEEDS      │     │    CLIENT     │     │   (Hyperliquid)│           │
│  └───────┬───────┘     └───────┬───────┘     └───────┬───────┘            │
│          │                     │                     │                      │
│          ▼                     ▼                     ▼                      │
│  ┌───────────────────────────────────────────────────────────────┐         │
│  │                      MARKET DATA LAYER                         │         │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │         │
│  │  │ CachedMid   │  │ Fill Queue  │  │ Connection Monitor   │   │         │
│  │  │ (500ms TTL) │  │ (WS+REST)   │  │ (Watchdog)          │   │         │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘   │         │
│  └───────────────────────────────────────────────────────────────┘         │
│                                  │                                          │
│                                  ▼                                          │
│  ┌───────────────────────────────────────────────────────────────┐         │
│  │                       CORE ENGINE                              │         │
│  │  ┌─────────────────────────────────────────────────────────┐  │         │
│  │  │                    FILL GUARANTOR                        │  │         │
│  │  │  - WAL first writes                                      │  │         │
│  │  │  - Deduplication                                         │  │         │
│  │  │  - Shadow ledger sync                                    │  │         │
│  │  └─────────────────────────────────────────────────────────┘  │         │
│  │                              │                                 │         │
│  │  ┌──────────────┬────────────┴────────────┬──────────────┐   │         │
│  │  ▼              ▼                         ▼              ▼   │         │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────┐   │         │
│  │  │Position  │ │  Grid    │ │   Order      │ │  Risk    │   │         │
│  │  │Tracker   │ │Calculator│ │ StateMachine │ │ Engine   │   │         │
│  │  └──────────┘ └──────────┘ └──────────────┘ └──────────┘   │         │
│  │                              │                                 │         │
│  └──────────────────────────────┼─────────────────────────────────┘         │
│                                 │                                           │
│                                 ▼                                           │
│  ┌───────────────────────────────────────────────────────────────┐         │
│  │                     ORDER ROUTER                               │         │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │         │
│  │  │ Coalescing  │  │ Rate Limit  │  │ Bulk Operations     │   │         │
│  │  │ (adaptive)  │  │ (per-coin)  │  │ (batched submit)    │   │         │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘   │         │
│  └───────────────────────────────────────────────────────────────┘         │
│                                 │                                           │
│                                 ▼                                           │
│  ┌───────────────────────────────────────────────────────────────┐         │
│  │                   PERSISTENCE LAYER                            │         │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │         │
│  │  │ Fill Log    │  │ State Store │  │ WAL (Write-Ahead)   │   │         │
│  │  │ (batched)   │  │ (atomic)    │  │                     │   │         │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘   │         │
│  └───────────────────────────────────────────────────────────────┘         │
│                                                                             │
└────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow for Fill Processing

```
                    FILL ARRIVES
                         │
                         ▼
            ┌────────────────────────┐
            │   Source Identification │
            │   (WS / REST / Recon)   │
            └────────────┬───────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │    Deduplication       │
            │    (fill_deduplicator) │
            └────────────┬───────────┘
                         │
              ┌──────────┴──────────┐
              │ New?                │
              │ Yes          No     │
              ▼              ▼      │
       ┌──────────┐    ┌──────────┐ │
       │Write WAL │    │  SKIP    │◄┘
       └────┬─────┘    └──────────┘
            │
            ▼
       ┌──────────────────────┐
       │ Acquire Position Lock │
       └──────────┬───────────┘
                  │
            ┌─────┴─────┐
            │ Order     │
            │ Lookup    │
            └─────┬─────┘
                  │
        ┌─────────┴─────────┐
        │ Found?            │
        │ Yes          No   │
        ▼              ▼    │
  ┌──────────┐   ┌──────────┐
  │ Update   │   │ Log      │
  │ Position │   │ Unmatched│
  │ + PnL    │   │ + Recon  │
  └────┬─────┘   └──────────┘
       │
       ▼
  ┌──────────────────────┐
  │ Release Position Lock │
  └──────────┬───────────┘
             │
             ▼
  ┌──────────────────────┐
  │ Replace Filled Order │
  │ (if not flatten mode)│
  └──────────┬───────────┘
             │
             ▼
  ┌──────────────────────┐
  │   Persist State      │
  └──────────────────────┘
```

---

# PRIORITIZED FIX LIST

## P0 - Critical (Fix Before Go-Live)

| ID | Issue | Effort | Impact |
|----|-------|--------|--------|
| C-1 | Race condition in fill/grid rebuild | 2h | HIGH - can cause wrong orders |
| C-2 | Partial fill 0.99 threshold bug | 30m | HIGH - missed final fills |
| C-3 | WS reconnect fill loss | 2h | HIGH - missed fills |
| C-4 | Grid spacing collapse | 1h | HIGH - self-trading risk |
| C-5 | Double position on replay | 1h | HIGH - wrong position |
| C-6 | Cancel failure leaves stale orders | 1h | HIGH - order leak |

## P1 - Major (Fix Within 1 Week)

| ID | Issue | Effort | Impact |
|----|-------|--------|--------|
| M-1 | No sequence tracking for multi-fills | 2h | MEDIUM |
| M-2 | REST poll can skip fills | 1h | MEDIUM |
| M-3 | Coalescing stales orders in volatility | 2h | MEDIUM |
| M-4 | Risk engine uses stale position | 1h | MEDIUM |
| M-5 | State not crash-safe | 4h | MEDIUM |
| M-6 | Float drift in price keys | 1h | LOW-MED |

## P2 - Structural (Next Release)

| ID | Issue | Effort | Impact |
|----|-------|--------|--------|
| S-1 | Implement shadow ledger | 8h | Quality improvement |
| S-2 | Event-driven refactor | 16h | Maintainability |
| S-3 | Order state machine integration | 4h | Consistency |

---

# PRE-LIVE CHECKLIST

## Before First Trade

- [ ] **C-1 Fixed**: Fill/grid race condition patched
- [ ] **C-2 Fixed**: Partial fill threshold updated
- [ ] **C-3 Fixed**: WS reconnect immediate REST poll
- [ ] **C-4 Fixed**: Grid spacing minimum enforced
- [ ] **C-5 Fixed**: Replay doesn't double-count position
- [ ] **C-6 Fixed**: Cancel failure checks order status

## Monitoring Setup

- [ ] Position drift alert enabled (>1% drift)
- [ ] Fill rate monitoring (compare WS vs REST counts)
- [ ] Grid width metrics being collected
- [ ] Circuit breaker status visible
- [ ] WAL write failures alerting

## Configuration Verified

- [ ] `HL_FILL_RESCAN_MS` set appropriately (recommend 10000)
- [ ] `HL_REST_FILL_POLL_SEC` < 30 seconds
- [ ] `HL_WS_STALE_AFTER_SEC` < 30 seconds
- [ ] `HL_MAX_SKEW_RATIO` configured for your risk tolerance
- [ ] `HL_MAX_DRAWDOWN_PCT` set with buffer

## Testing Completed

- [ ] Fast wick simulation (rapid price movement)
- [ ] WS disconnect recovery test
- [ ] Partial fill sequence test
- [ ] Grid compression stress test
- [ ] Crash recovery test (kill during fill)

## Runtime Validation

- [ ] Compare local position with `info.user_state()` every 5 minutes
- [ ] Compare local fill count with exchange fill history daily
- [ ] Check for stuck orders (PENDING > 30s) in state machine
- [ ] Monitor `position_drift_detected` counter
- [ ] Watch `grid_compression_prevented` events

---

# APPENDIX: METRIC RECOMMENDATIONS

## Missing Critical Metrics

```python
# Add to metrics_rich.py

# Fill capture accuracy
self.fills_from_ws = Counter('fills_from_ws_total', ...)
self.fills_from_rest = Counter('fills_from_rest_total', ...)
self.fills_from_recon = Counter('fills_from_reconciliation_total', ...)

# Order lifecycle
self.order_pending_duration_ms = Histogram('order_pending_duration_ms', ...)
self.orders_stuck_count = Gauge('orders_stuck_in_pending', ...)

# Grid health
self.grid_level_gap_pct = Gauge('grid_level_gap_pct', ...)  # Spacing between levels
self.grid_coverage_pct = Gauge('grid_coverage_pct', ...)    # % of target levels active

# Shadow ledger
self.shadow_ledger_drift = Gauge('shadow_ledger_drift', ...)
self.reconciliation_corrections = Counter('reconciliation_corrections_total', ...)
```

## Dashboard Schema

```json
{
  "panels": [
    {
      "title": "Position & PnL",
      "metrics": ["position", "realized_pnl", "unrealized_pnl", "daily_pnl"]
    },
    {
      "title": "Fill Capture",
      "metrics": ["fills_total", "fills_from_ws", "fills_from_rest", "fill_source_mismatch"]
    },
    {
      "title": "Grid Health",
      "metrics": ["grid_width_pct", "grid_center", "skew_ratio", "orders_active"]
    },
    {
      "title": "System Health",
      "metrics": ["api_error_streak", "ws_gaps_total", "position_drift_amount"]
    },
    {
      "title": "Risk Status",
      "metrics": ["risk_halted", "max_drawdown_pct", "margin_utilization_pct"]
    }
  ]
}
```

---

*Review completed. Implement P0 fixes before any live trading.*
