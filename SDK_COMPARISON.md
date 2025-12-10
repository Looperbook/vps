# Hyperliquid Python SDK Comparison Report

## Executive Summary

This document compares our gridbot implementation against the official [hyperliquid-dex/hyperliquid-python-sdk](https://github.com/hyperliquid-dex/hyperliquid-python-sdk) to identify inconsistencies, missing features, and best practice deviations.

**Overall Assessment**: Our implementation is **well-aligned** with SDK patterns. Several minor improvements identified below.

---

## ✅ Correctly Implemented (SDK-Compliant)

### 1. Order Placement
| Feature | SDK Pattern | Our Implementation | Status |
|---------|-------------|-------------------|--------|
| `order()` signature | `order(name, is_buy, sz, limit_px, order_type, reduce_only, cloid, builder)` | Matches via `exchange.order()` | ✅ |
| `bulk_orders()` | `bulk_orders(order_requests, builder, grouping)` | Matches via `exchange.bulk_orders()` | ✅ |
| Order type format | `{"limit": {"tif": "Gtc"}}` | Correctly used in `order_router.py` | ✅ |
| Cloid usage | `Cloid.from_str("0x...")` or `Cloid.from_int(n)` | Correctly using `Cloid(f"0x{secrets.token_hex(16)}")` | ✅ |

### 2. Cancellation
| Feature | SDK Pattern | Our Implementation | Status |
|---------|-------------|-------------------|--------|
| `cancel(coin, oid)` | Returns via `bulk_cancel()` | Matches in `async_execution.py` | ✅ |
| `cancel_by_cloid(coin, cloid)` | Returns via `bulk_cancel_by_cloid()` | Matches in `async_execution.py` | ✅ |
| `bulk_cancel()` | `[{"coin": name, "oid": oid}]` | Available in `async_execution.py` | ✅ |

### 3. WebSocket Subscriptions
| Feature | SDK Pattern | Our Implementation | Status |
|---------|-------------|-------------------|--------|
| `userFills` | `{"type": "userFills", "user": address}` | Correct in `market_data.py` | ✅ |
| `l2Book` | `{"type": "l2Book", "coin": coin}` | Correct in `market_data.py` | ✅ |
| `allMids` | `{"type": "allMids", "dex": dex}` | Correct (uses "dex" not "perpDex") | ✅ |

### 4. REST API
| Feature | SDK Pattern | Our Implementation | Status |
|---------|-------------|-------------------|--------|
| `all_mids(dex=)` | `{"type": "allMids", "dex": dex}` | Correct in `async_info.py` | ✅ |
| `user_state(address, dex=)` | `{"type": "clearinghouseState", "user": ..., "dex": ...}` | Correct in `async_info.py` | ✅ |
| `frontend_open_orders(address, dex=)` | Supports `dex` parameter | Correct in `async_info.py` | ✅ |
| `user_fills_by_time(address, start_ms)` | No `dex` parameter | Correct (we fixed this) | ✅ |

### 5. Price Rounding
| Feature | SDK Pattern | Our Implementation | Status |
|---------|-------------|-------------------|--------|
| Sig figs | `float(f"{px:.5g}")` then round to decimals | `round_price()` in `rounding.py` | ✅ |
| Tick alignment | SDK doesn't enforce, user responsibility | `snap_to_tick()` in `rounding.py` | ✅ |

---

## ⚠️ Minor Deviations / Improvements Possible

### 1. **WebSocket Ping Interval**
- **SDK**: Pings every **50 seconds** (`PING_INTERVAL = 50`)
- **Our Code**: We rely on SDK's internal ping handling
- **Risk**: Low (SDK handles this internally)
- **Recommendation**: No action needed - SDK manages pings

### 2. **`expires_after` Support**
- **SDK**: `Exchange.set_expires_after(expires_after: int)` - causes actions to be rejected after timestamp
- **Our Code**: Not using `expires_after`
- **Risk**: Low - only useful for time-sensitive order scenarios
- **Recommendation**: Consider for institutional-grade safety:
```python
# Optional: Set order expiry to prevent stale orders from executing
exchange.set_expires_after(get_timestamp_ms() + 60000)  # 60s validity
```

### 3. **`schedule_cancel` Feature**
- **SDK**: `exchange.schedule_cancel(time_ms)` - cancels all orders at specified time
- **Our Code**: Not using this feature
- **Risk**: Medium - useful for kill-switch scenarios
- **Recommendation**: Add as safety feature:
```python
# Schedule emergency cancel 30 minutes from now as dead-man switch
exchange.schedule_cancel(get_timestamp_ms() + 1800000)
```

### 4. **Order Modify Support**
- **SDK**: `exchange.modify_order(oid, coin, is_buy, sz, limit_px, order_type, cloid)`
- **Our Code**: Cancel + re-place instead of modify
- **Risk**: Low - cancel+replace is more reliable
- **Trade-off**: Modify is atomic but can fail silently; cancel+replace is 2 operations but explicit
- **Recommendation**: Keep current approach for reliability

### 5. **`query_order_by_oid` / `query_order_by_cloid`**
- **SDK**: `info.query_order_by_oid(user, oid)` and `info.query_order_by_cloid(user, cloid)`
- **Our Code**: Not using these for order status verification
- **Recommendation**: Add to `async_info.py` for reconciliation:
```python
async def query_order_by_cloid(self, account: str, cloid: str) -> Any:
    payload = {"type": "orderStatus", "user": account, "oid": cloid}
    return await self._post_info(payload)
```

### 6. **`user_rate_limit` Query**
- **SDK**: `info.user_rate_limit(user)` - returns current rate limit status
- **Our Code**: Not querying rate limits
- **Recommendation**: Add for monitoring/alerting:
```python
async def user_rate_limit(self, account: str) -> Any:
    payload = {"type": "userRateLimit", "user": account}
    return await self._post_info(payload)
```

---

## 🔴 Missing Features (Consider Adding)

### 1. **`orderUpdates` WebSocket Subscription** (Priority: HIGH)
- **SDK**: Subscribe to `{"type": "orderUpdates", "user": address}`
- **Benefit**: Real-time order status changes (filled, cancelled, etc.)
- **Current**: We rely on `userFills` + REST polling
- **Recommendation**: Add for faster order state synchronization
```python
sub_orders = self.info.subscribe(
    {"type": "orderUpdates", "user": self.account}, 
    _on_order_update
)
```

### 2. **Grouping for TP/SL Orders** (Priority: MEDIUM)
- **SDK**: `bulk_orders(..., grouping="normalTpsl")` or `grouping="positionTpsl"`
- **Benefit**: Atomic placement of parent order + TP/SL children
- **Current**: Not using grouping (we use simple grid orders)
- **Recommendation**: Keep as-is for grid strategy; add if implementing TP/SL

### 3. **Builder Fee Support** (Priority: LOW)
- **SDK**: `order(..., builder={"b": "0x...", "f": 10})`
- **Benefit**: Support builder fee programs
- **Current**: Not passing builder info
- **Recommendation**: Add if participating in builder programs:
```python
# Builder info: address and fee in tenths of basis points (10 = 1bp)
builder = {"b": "0xBuilderAddress", "f": 10}
exchange.order(coin, is_buy, sz, px, order_type, builder=builder)
```

### 4. **`bbo` Subscription** (Priority: LOW)
- **SDK**: `{"type": "bbo", "coin": coin}` - best bid/offer only
- **Benefit**: More efficient than full l2Book for mid-price
- **Current**: Using `allMids` + `l2Book`
- **Recommendation**: Consider for lower bandwidth:
```python
sub_bbo = self.info.subscribe({"type": "bbo", "coin": self.coin}, _on_bbo)
```

### 5. **Market Orders with Slippage** (Priority: MEDIUM)
- **SDK**: `exchange.market_open(coin, is_buy, sz, px=None, slippage=0.05)`
- **Benefit**: Convenient market order with slippage protection
- **Current**: Using limit orders only (correct for grid strategy)
- **Recommendation**: Add for emergency position closing:
```python
# Emergency market close with 5% slippage
await async_exchange.market_close(coin, sz=abs(position), slippage=0.05)
```

---

## 📋 Implementation Checklist

### High Priority (Recommended)
- [ ] Add `orderUpdates` WebSocket subscription for faster order sync
- [ ] Add `query_order_by_cloid()` to `async_info.py` for order verification
- [ ] Consider `schedule_cancel()` as dead-man safety switch

### Medium Priority (Nice to Have)
- [ ] Add `user_rate_limit()` query for monitoring
- [ ] Add `market_close()` wrapper for emergency exits
- [ ] Add `expires_after` support for order validity windows

### Low Priority (Future Enhancement)
- [ ] Add `bbo` subscription option for bandwidth optimization
- [ ] Add builder fee support if participating in builder programs
- [ ] Add `historical_orders()` for trade history analysis

---

## SDK Response Handling Patterns

Our `_extract_ids()` and `_extract_error_message()` methods correctly handle SDK response shapes:

```python
# SDK response patterns we correctly handle:
{"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 123, "cloid": "0x..."}}]}}}
{"status": "ok", "response": {"data": {"statuses": [{"filled": {"oid": 123}}]}}}
{"status": "err", "response": "error message"}
{"response": {"data": {"statuses": [{"error": "reason"}]}}}
```

---

## Conclusion

Our implementation is **highly compliant** with the official Hyperliquid Python SDK. The main areas for improvement are:

1. **Add `orderUpdates` subscription** - This would give us real-time order state changes instead of relying solely on fill events and REST polling.

2. **Add `schedule_cancel()` as safety feature** - Useful as a dead-man switch for emergency scenarios.

3. **Add order query methods** - `query_order_by_cloid()` would help with reconciliation.

The current implementation correctly:
- Uses proper API parameter names (`dex` not `perpDex`)
- Handles response parsing for all SDK response shapes
- Implements proper price rounding and tick alignment
- Uses cloids correctly for order tracking
- Handles WebSocket subscriptions per SDK patterns

No critical bugs or misalignments found.
