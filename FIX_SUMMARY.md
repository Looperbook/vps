# GridBot Critical Fix Summary

## Quick Reference: What's Wrong and How to Fix It

### P0 Critical Issues (Must Fix Before Live)

| Issue | Root Cause | Risk | Fix Location |
|-------|------------|------|--------------|
| **C-1: Race Condition** | Fill can change position during grid rebuild | Wrong grid orders placed | `bot.py:_build_and_place_grid` |
| **C-2: Partial Fill Bug** | 0.99% threshold misses tiny final fills | Orders stuck in registry | `order_manager.py:handle_partial_fill` |
| **C-3: WS Reconnect Gap** | No immediate REST poll after WS reconnect | Missed fills during reconnect | `market_data.py`, `bot.py:run` |
| **C-4: Grid Collapse** | Loss-drift tightening can go to 50% | Self-trading risk | `strategy.py:compute_spacing` |
| **C-5: Double Position** | Replay fills also update position | Wrong position on restart | `bot.py:_handle_fill` |
| **C-6: Cancel Leak** | Failed cancel leaves order in registry | Stale orders accumulate | `bot.py:_cancel_record` |

---

## Visual Architecture

```
                                 ┌─────────────────┐
                                 │   HYPERLIQUID   │
                                 │    EXCHANGE     │
                                 └────────┬────────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    │                     │                     │
                    ▼                     ▼                     ▼
            ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
            │  WebSocket   │     │   REST API   │     │   User       │
            │  Fills Feed  │     │   Polling    │     │   State      │
            └──────┬───────┘     └──────┬───────┘     └──────┬───────┘
                   │                    │                    │
                   └────────────────────┼────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            FILL GUARANTOR (New)                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 1. Dedup Check → 2. WAL Write → 3. Ledger Update → 4. Process      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
          ┌─────────────────────────────┼─────────────────────────────┐
          │                             │                             │
          ▼                             ▼                             ▼
┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
│  Shadow Ledger   │        │  Order Manager   │        │   Risk Engine    │
│  (New)           │        │                  │        │                  │
│ • local_position │        │ • by_cloid index │        │ • allow_order()  │
│ • pending orders │        │ • by_oid index   │        │ • drawdown check │
│ • drift detect   │        │ • by_price index │        │ • velocity limit │
└──────────────────┘        └──────────────────┘        └──────────────────┘
          │                             │                             │
          └─────────────────────────────┼─────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GRID CALCULATOR                                 │
│  • compute_spacing() - with collapse protection (C-4 FIX)                   │
│  • build_filtered_levels() - risk-checked                                   │
│  • compute_grid_diff() - smart cancel/place                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                               ORDER ROUTER                                   │
│  • Coalescing (adaptive) │ Rate limiting │ Bulk batching │ Nonce safety    │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                                 ┌─────────────────┐
                                 │   HYPERLIQUID   │
                                 │    EXCHANGE     │
                                 └─────────────────┘
```

---

## Fill Processing Flow (After Patches)

```
     FILL ARRIVES
          │
          ▼
   ┌──────────────┐
   │ Dedup Check  │──── Duplicate? ────► SKIP
   └──────┬───────┘
          │ New
          ▼
   ┌──────────────┐
   │  Write WAL   │ ◄── CRASH SAFE POINT
   └──────┬───────┘
          │
          ▼
   ┌──────────────┐
   │  Is Replay?  │──── Yes ────► Clear order only (C-5 FIX)
   └──────┬───────┘                    │
          │ No (Live)                  │
          ▼                            │
   ┌──────────────┐                    │
   │ Acquire Lock │                    │
   └──────┬───────┘                    │
          │                            │
          ▼                            │
   ┌──────────────┐                    │
   │ Find Order   │──── Not Found ────► Reconcile + Log
   └──────┬───────┘
          │ Found
          ▼
   ┌──────────────┐
   │ Update Qty   │──── Partial? ────► Keep in registry (C-2 FIX)
   └──────┬───────┘
          │ Complete
          ▼
   ┌──────────────┐
   │ Unindex Ord  │
   │ Update Pos   │
   │ Calc PnL     │
   └──────┬───────┘
          │
          ▼
   ┌──────────────┐
   │ Release Lock │
   └──────┬───────┘
          │
          ▼
   ┌──────────────┐
   │Replace Order │
   └──────┬───────┘
          │
          ▼
   ┌──────────────┐
   │   Persist    │
   └──────────────┘
```

---

## Grid Build Flow (After C-1 Fix)

```
     MID PRICE AVAILABLE
            │
            ▼
    ┌───────────────────┐
    │ ACQUIRE POS LOCK  │ ◄── CRITICAL: Hold through computation
    └────────┬──────────┘
             │
             ▼
    ┌───────────────────┐
    │ Snapshot Position │
    └────────┬──────────┘
             │
             ▼
    ┌───────────────────┐
    │ Compute Spacing   │ ◄── With collapse protection (C-4)
    └────────┬──────────┘
             │
             ▼
    ┌───────────────────┐
    │ Build Levels      │
    │ (under lock)      │
    └────────┬──────────┘
             │
             ▼
    ┌───────────────────┐
    │ Compute Diff      │
    │ (under lock)      │
    └────────┬──────────┘
             │
             ▼
    ┌───────────────────┐
    │ RELEASE POS LOCK  │
    └────────┬──────────┘
             │
             ▼
    ┌───────────────────┐
    │ Cancel Old Orders │
    └────────┬──────────┘
             │
             ▼
    ┌───────────────────┐
    │ Check Drift       │──── >10% drift? ────► ABORT + Reschedule
    └────────┬──────────┘
             │ OK
             ▼
    ┌───────────────────┐
    │ Place New Orders  │
    └───────────────────┘
```

---

## Implementation Checklist

### Phase 1: Critical Fixes (Day 1)
- [ ] Apply C-1: Race condition fix in `_build_and_place_grid`
- [ ] Apply C-2: Partial fill threshold fix
- [ ] Apply C-5: Replay position fix
- [ ] Run existing test suite
- [ ] Manual test: Rapid fill simulation

### Phase 2: Fill Safety (Day 2)  
- [ ] Apply C-3: WS reconnect immediate poll
- [ ] Apply C-6: Cancel failure check
- [ ] Add position drift metric alert
- [ ] Test: WS disconnect/reconnect scenario

### Phase 3: Grid Safety (Day 3)
- [ ] Apply C-4: Grid spacing collapse protection
- [ ] Add grid width monitoring
- [ ] Test: High volatility simulation
- [ ] Test: Loss-drift tightening limits

### Phase 4: Enhanced Monitoring (Week 1)
- [ ] Add `fills_from_ws` / `fills_from_rest` counters
- [ ] Add `shadow_ledger_drift` gauge
- [ ] Add `grid_level_gap_pct` gauge
- [ ] Set up alerting on position drift >1%

### Phase 5: Structural (Week 2-3)
- [ ] Implement Shadow Ledger module
- [ ] Implement Fill Guarantor module
- [ ] Wire up event bus pattern
- [ ] Full integration testing

---

## Monitoring Dashboard Schema

```yaml
# Recommended Grafana panels

row_1_position_pnl:
  - position: gauge by coin
  - realized_pnl: counter by coin
  - daily_pnl: gauge
  - unrealized_pnl: gauge (position * (mid - entry))

row_2_fill_capture:
  - fills_total: counter by side
  - fills_from_ws: counter (new metric)
  - fills_from_rest: counter (new metric)
  - fill_source_ratio: ws/total (should be >95%)

row_3_grid_health:
  - grid_width_pct: gauge
  - skew_ratio: gauge
  - orders_active: gauge
  - grid_center: gauge vs mid

row_4_system_health:
  - api_error_streak: gauge with alert >3
  - position_drift_amount: gauge with alert >1%
  - ws_data_age: gauge with alert >20s
  - circuit_breaker_tripped: binary

row_5_risk:
  - risk_halted: binary
  - max_drawdown_pct: gauge vs limit
  - margin_utilization: gauge
  - funding_paid: cumulative
```

---

## Quick Commands

```powershell
# Run tests
pytest tests/ -v

# Run specific critical tests
pytest tests/test_production_hardening.py -v

# Check for position drift in logs
Select-String "position_drift" logs/*.log

# Monitor grid compression events
Select-String "grid_compression" logs/*.log | Select-Object -Last 20

# Check fill sources
Select-String '"event":"fill"' logs/*.log | Measure-Object
```

---

## Emergency Procedures

### If Position Drifts
1. Check `position_drift_detected` events in logs
2. Force reconciliation: set `last_resync = 0` to trigger immediate sync
3. If >10% drift: stop bot, manually reconcile, restart

### If Fills Missing
1. Check `fill_source_ratio` - if REST > WS, WS is broken
2. Force WS reconnect by stopping/starting market data
3. Trigger manual REST fill poll for last hour

### If Grid Collapses
1. Check `grid_compression_override` events
2. Temporarily increase `HL_GRID_SPACING_PCT`
3. Reduce `HL_NUM_GRIDS` to give more room
4. Check `loss_drift` values - may need to flatten

---

*This document should be kept with the codebase and updated as fixes are applied.*
