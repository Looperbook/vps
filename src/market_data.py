"""
Market data adapter using Hyperliquid Info: WS fills + ticker mids.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Dict, List, Optional
import json
import logging
import random

from hyperliquid.info import Info

from src.config import Settings
from src.utils import BoundedSet


class CachedMid:
    """
    Optimization-1: Thread-safe cached mid-price with TTL to reduce REST calls.
    
    When WS feeds provide fresh data, we cache it here. Subsequent reads within
    the TTL window return the cached value without hitting REST endpoints.
    """
    __slots__ = ("_value", "_timestamp_ms", "_ttl_ms", "_lock")

    def __init__(self, ttl_ms: int = 500) -> None:
        self._value: float = 0.0
        self._timestamp_ms: int = 0
        self._ttl_ms: int = ttl_ms
        self._lock = threading.Lock()

    def get(self) -> Optional[float]:
        """Return cached value if within TTL, else None."""
        with self._lock:
            now_ms = int(time.time() * 1000)
            if self._value > 0 and (now_ms - self._timestamp_ms) < self._ttl_ms:
                return self._value
            return None

    def set(self, value: float) -> None:
        """Update cached value with current timestamp."""
        if value <= 0:
            return
        with self._lock:
            self._value = value
            self._timestamp_ms = int(time.time() * 1000)

    def get_unchecked(self) -> float:
        """Return last known value regardless of TTL (for fallback scenarios)."""
        with self._lock:
            return self._value

    def age_ms(self) -> int:
        """Return age of cached value in milliseconds."""
        with self._lock:
            if self._timestamp_ms == 0:
                return 0
            return int(time.time() * 1000) - self._timestamp_ms


class MarketData:
    def __init__(self, info: Info, coin: str, account: str, cfg: Settings, async_info=None,
                 on_reconnect: Optional[callable] = None, on_order_update: Optional[callable] = None) -> None:
        self.info = info
        self.async_info = async_info
        self.coin = coin
        self.account = account
        self.dex = cfg.dex
        self._http_timeout = cfg.http_timeout
        self.fill_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        # SDK Feature: orderUpdates queue for real-time order state changes
        self.order_update_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self.last_event: float = time.time()
        self.ws_started = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._fill_seen = BoundedSet(maxlen=5000)
        # Critical-9: Thread-safe lock for _fill_seen access from WS callback thread
        self._fill_seen_lock = threading.Lock()
        self._last_mid: float = 0.0
        self._last_mid_ts: float = 0.0
        self._last_rest_mid_ts: float = 0.0
        # Optimization-1: Use CachedMid for smart caching with configurable TTL
        self._cached_mid = CachedMid(ttl_ms=cfg.mid_cache_ttl_ms)
        self._watchdog_task: Optional[asyncio.Task] = None
        self._subs: Dict[str, int] = {}
        self._stale_after = cfg.ws_stale_after
        self._fatal_after = cfg.data_halt_sec
        self._watch_interval = cfg.ws_watch_interval
        self._backoff_max = 60
        self._rest_mid_backoff = cfg.rest_mid_backoff_sec
        self._best_bid: float = 0.0
        self._best_ask: float = 0.0
        self._halted = False
        self._last_warn_ts = 0.0
        self._stopping = False
        self._last_resubscribe = 0.0
        self._max_queue = 2000  # guardrail to avoid unbounded fill growth
        # Track last WS message received (any message) to distinguish "WS dead" from "no data for our coin"
        self._last_ws_any_msg: float = time.time()
        self._last_mid_log_ts = 0.0
        self._last_mid_log_val = 0.0
        # Filter WS fills older than this timestamp (set by bot after loading state)
        self.min_fill_time_ms: int = 0
        # C-3 FIX: Callback to trigger REST poll on WS reconnect
        # This ensures we catch fills missed during WS disconnect gap
        self._on_reconnect = on_reconnect
        # SDK Feature: Callback for order updates (filled, cancelled, etc.)
        self._on_order_update = on_order_update

    def _log_mid(self, source: str, mid: float) -> None:
        """
        Throttle mid-price logging to reduce noise: only log if >0.1% change or every 30s.
        Uses DEBUG level since mid prices are high-frequency diagnostic data.
        """
        now = time.time()
        should_log = False
        if self._last_mid_log_val == 0.0:
            should_log = True
        else:
            delta = abs(mid - self._last_mid_log_val)
            rel = delta / self._last_mid_log_val if self._last_mid_log_val else 1.0
            if rel >= 0.001:  # 10 bps move (was 5 bps - too noisy)
                should_log = True
        if now - self._last_mid_log_ts >= 30.0:  # Every 30s instead of 15s
            should_log = True
        if should_log:
            self._last_mid_log_ts = now
            self._last_mid_log_val = mid
            logging.getLogger("gridbot").debug(json.dumps({"event": f"mid_price_{source}", "coin": self.coin, "mid": mid}))

    def _check_and_add_fill(self, key: str) -> bool:
        """Critical-9: Thread-safe fill dedup check for WS callback thread."""
        with self._fill_seen_lock:
            return self._fill_seen.add(key)

    def start_ws(self, loop: asyncio.AbstractEventLoop) -> None:
        if self.ws_started:
            return
        self.loop = loop

        def _mark_healthy() -> None:
            self.last_event = time.time()
            if self._halted:
                self._halted = False
                logging.getLogger("gridbot").info(json.dumps({"event": "ws_resume", "coin": self.coin}))

        def _on_user_fills(msg: Any) -> None:
            try:
                data = msg.get("data", {})
                if data.get("user", "").lower() != self.account.lower():
                    return
                fills = data.get("fills") or []
                for f in fills:
                    if f.get("coin") != self.coin:
                        continue
                    # Skip fills older than min_fill_time_ms to avoid processing stale WS snapshot
                    fill_ts = int(f.get("time", 0))
                    if self.min_fill_time_ms > 0 and fill_ts < self.min_fill_time_ms:
                        continue
                    key = self._fill_key(f)
                    # Critical-9: Use thread-safe method
                    if not self._check_and_add_fill(key):
                        continue
                    _mark_healthy()
                    if self.loop:
                        asyncio.run_coroutine_threadsafe(self.fill_queue.put(f), self.loop)
            except Exception:
                # Do not raise from callback
                pass

        def _on_order_update(msg: Any) -> None:
            """
            SDK Feature: Handle orderUpdates subscription for real-time order state changes.
            This provides faster notification of fills, cancellations, and order status changes.
            """
            try:
                self._last_ws_any_msg = time.time()
                data = msg.get("data", {})
                if not data:
                    return
                # orderUpdates can contain multiple order statuses
                updates = data if isinstance(data, list) else [data]
                for update in updates:
                    # Filter to our coin
                    coin = update.get("coin") or update.get("order", {}).get("coin")
                    if coin and coin != self.coin:
                        continue
                    _mark_healthy()
                    if self.loop:
                        asyncio.run_coroutine_threadsafe(self.order_update_queue.put(update), self.loop)
                    # Also invoke callback if registered
                    if self._on_order_update:
                        try:
                            if self.loop:
                                asyncio.run_coroutine_threadsafe(self._on_order_update(update), self.loop)
                        except Exception:
                            pass
            except Exception:
                pass

        def _on_ticker(msg: Any) -> None:
            try:
                # Mark WS as alive on ANY message (even if not for our coin)
                # This prevents false stale detection for low-volume builder DEX markets
                self._last_ws_any_msg = time.time()
                
                data = msg.get("data", {})
                levels = data.get("levels")
                if levels and data.get("coin") == self.coin:
                    bids = levels.get("bids") or []
                    asks = levels.get("asks") or []
                    if bids and isinstance(bids[0], list):
                        self._best_bid = float(bids[0][0])
                    if asks and isinstance(asks[0], list):
                        self._best_ask = float(asks[0][0])
                    if self._best_bid and self._best_ask:
                        self._last_mid = (self._best_bid + self._best_ask) / 2
                        self._last_mid_ts = time.time()
                        _mark_healthy()
                        return
                mids = data.get("allMids", {})
                mid = mids.get(self.coin)
                if mid is None:
                    return
                self._last_mid = float(mid)
                self._last_mid_ts = time.time()
                _mark_healthy()
            except Exception:
                # swallow ticker errors to keep WS alive
                pass

        # Subscribe to WebSocket feeds per Hyperliquid docs:
        # - userFills: user address only (no dex parameter in docs)
        # - l2Book: coin only (no dex parameter in docs)  
        # - allMids: uses "dex" field (not "perpDex") for builder DEX
        # - orderUpdates: user address only (SDK feature for real-time order state)
        sub_fills = self.info.subscribe({"type": "userFills", "user": self.account}, _on_user_fills)
        sub_book = None
        try:
            sub_book = self.info.subscribe({"type": "l2Book", "coin": self.coin}, _on_ticker)
        except Exception:
            sub_book = None
        # Per docs: allMids uses "dex" key for perp dex selection
        sub_mids = self.info.subscribe({"type": "allMids", "dex": self.dex}, _on_ticker)
        # SDK Feature: Subscribe to orderUpdates for real-time order state changes
        sub_orders = None
        try:
            sub_orders = self.info.subscribe({"type": "orderUpdates", "user": self.account}, _on_order_update)
        except Exception as e:
            logging.getLogger("gridbot").warning(json.dumps({"event": "ws_orderUpdates_sub_failed", "err": str(e)}))
        self._subs = {"fills": sub_fills, "mids": sub_mids}
        if sub_book is not None:
            self._subs["book"] = sub_book
        if sub_orders is not None:
            self._subs["orders"] = sub_orders
        logging.getLogger("gridbot").info(json.dumps({"event": "ws_start", "coin": self.coin, "account": self.account, "dex": self.dex, "has_order_updates": sub_orders is not None}))
        self.ws_started = True
        # PRODUCTION EXCELLENCE NOTE:
        # Added WS watchdog with backoff to avoid silent data loss on dropped sockets.
        if self.loop:
            self._watchdog_task = asyncio.create_task(self._watchdog())

    async def stop(self) -> None:
        self._stopping = True
        if self._watchdog_task:
            self._watchdog_task.cancel()
            await asyncio.gather(self._watchdog_task, return_exceptions=True)
            self._watchdog_task = None
        try:
            for key, sub_id in list(self._subs.items()):
                try:
                    self.info.unsubscribe({"type": "noop"}, sub_id)
                except Exception:
                    pass
            self._subs.clear()
        except Exception:
            pass

    async def next_fill(self) -> Dict[str, Any]:
        return await self.fill_queue.get()

    def _fill_key(self, f: Dict[str, Any]) -> str:
        oid = str(f.get("oid", ""))
        side = f.get("side", "")[:1]
        px = f.get("px", "")
        sz = f.get("sz", "")
        t = f.get("time", "")
        return f"{oid}_{side}_{px}_{sz}_{t}"

    async def mid_price(self) -> float:
        # Optimization-1: First check the smart cache (thread-safe, TTL-aware)
        cached = self._cached_mid.get()
        if cached is not None:
            self._log_mid("cache", cached)
            return cached
        # Prefer WS mid if fresh (within stale threshold), else REST fallback
        if self._last_mid and (time.time() - self._last_mid_ts) < self._stale_after:
            self._cached_mid.set(self._last_mid)  # Populate cache for next call
            self._log_mid("ws", self._last_mid)
            return self._last_mid
        if self._best_bid and self._best_ask:
            mid_val = (self._best_bid + self._best_ask) / 2
            self._cached_mid.set(mid_val)  # Populate cache
            self._log_mid("book", mid_val)
            return mid_val
        now_t = time.time()
        if self._last_mid and now_t - self._last_rest_mid_ts < self._rest_mid_backoff:
            # HARDENED: avoid hammering REST during outages; return last known mid
            logging.getLogger("gridbot").info(json.dumps({"event": "rest_mid_backoff", "coin": self.coin, "age": now_t - self._last_mid_ts}))
            return self._last_mid

        try:
            if self.async_info:
                try:
                    mids = await self.async_info.all_mids(self.dex)
                    mid = float(mids.get(self.coin, 0.0))
                    if mid <= 0:
                        raise ValueError("async mid <= 0")
                except Exception as exc_async:
                    logging.getLogger("gridbot").warning(json.dumps({"event": "mid_price_async_error", "coin": self.coin, "err": str(exc_async)}))
                    def _call() -> float:
                        mids_sync = self.info.all_mids(dex=self.dex)
                        mid_str = mids_sync.get(self.coin, "0")
                        return float(mid_str)
                    mid = await self._call_with_retry(_call, label="all_mids")
            else:
                def _call() -> float:
                    mids = self.info.all_mids(dex=self.dex)
                    mid_str = mids.get(self.coin, "0")
                    return float(mid_str)
                mid = await self._call_with_retry(_call, label="all_mids")
        except Exception as exc:
            logging.getLogger("gridbot").error(json.dumps({"event": "mid_price_rest_error", "coin": self.coin, "err": str(exc)}))
            raise
        self._last_mid = mid
        self._last_mid_ts = time.time()
        self.last_event = self._last_mid_ts  # REST success counts as healthy
        self._last_rest_mid_ts = self._last_mid_ts
        # Optimization-1: Populate cache after successful REST fetch
        self._cached_mid.set(mid)
        if mid <= 0:
            logging.getLogger("gridbot").error(json.dumps({"event": "mid_price_invalid", "coin": self.coin, "mid": mid}))
            raise RuntimeError(f"invalid mid {mid} for {self.coin}")
        self._log_mid("rest", mid)
        return mid

    async def user_fills_since(self, account: str, start_ms: int) -> List[Dict[str, Any]]:
        """
        Fetch user fills since a given timestamp.
        
        Per Hyperliquid docs: userFillsByTime does NOT support dex filtering.
        We fetch all fills and filter client-side by coin.
        """
        if self.async_info:
            res = await self.async_info.user_fills_by_time(account, start_ms)
        else:
            def _call() -> List[Dict[str, Any]]:
                # Per SDK: user_fills_by_time(address, start_time, end_time=None, aggregate_by_time=False)
                # No dex parameter - returns all fills for user
                res = self.info.user_fills_by_time(account, start_ms)
                if isinstance(res, dict) and "fills" in res:
                    return res["fills"]
                if isinstance(res, list):
                    return res
                return []
            res = await self._call_with_retry(_call, label="user_fills_by_time")
        if isinstance(res, dict) and "fills" in res:
            fills = res["fills"]
        elif isinstance(res, list):
            fills = res
        else:
            fills = []
        # Filter to only our coin (userFillsByTime returns all fills across all coins/dexs)
        return [f for f in fills if f.get("coin") == self.coin]

    async def _call_with_retry(self, fn, label: str, retries: int = 1):
        """
        Execute blocking Info call in a thread with timeout and limited retries.
        """
        backoff = 0.25
        for attempt in range(retries + 1):
            try:
                loop = asyncio.get_running_loop()
                return await asyncio.wait_for(loop.run_in_executor(None, fn), timeout=self._http_timeout)
            except Exception as exc:
                logging.getLogger("gridbot").warning(
                    json.dumps({"event": "http_retry", "coin": self.coin, "where": label, "err": str(exc), "attempt": attempt})
                )
                if attempt >= retries:
                    raise
                await asyncio.sleep(backoff)
                backoff *= 2

    async def _watchdog(self) -> None:
        """
        Monitor websocket freshness; resubscribe with exponential backoff if stale.
        """
        backoff = 5.0
        log = logging.getLogger("gridbot")
        while True:
            try:
                await asyncio.sleep(self._watch_interval)
                if self._stopping:
                    break
                # Backpressure: drop oldest fills if queue explodes to avoid OOM
                if self.fill_queue.qsize() > self._max_queue:
                    try:
                        for _ in range(self.fill_queue.qsize() - self._max_queue):
                            self.fill_queue.get_nowait()
                            self.fill_queue.task_done()
                        log.warning(json.dumps({"event": "fill_queue_pruned", "coin": self.coin, "size": self.fill_queue.qsize()}))
                    except Exception:
                        pass
                gap = time.time() - self.last_event
                if gap < self._stale_after:
                    backoff = 5.0
                    self._halted = False
                    continue
                now = time.time()
                # avoid hammering resubscribe if we recently refreshed via REST
                if (now - self._last_rest_mid_ts) < self._stale_after:
                    continue
                # FIX: For builder DEX markets, WS may be alive but not sending our coin
                # Only warn if WS itself is truly stale (no messages at all)
                ws_alive_gap = now - self._last_ws_any_msg
                if ws_alive_gap < self._stale_after:
                    # WS is alive, just no data for our coin - this is normal for low-volume markets
                    # Reset backoff since WS is healthy
                    backoff = 5.0
                    self._halted = False
                    continue
                if now - self._last_warn_ts >= max(self._watch_interval, backoff):
                    # HARDENED: throttle stale warnings; mark halt if fatal gap exceeded.
                    log.warning(json.dumps({"event": "ws_stale_detected", "coin": self.coin, "gap_sec": gap, "ws_gap_sec": ws_alive_gap, "backoff": backoff}))
                    self._last_warn_ts = now
                if gap >= self._fatal_after and not self._halted:
                    self._halted = True
                    log.warning(json.dumps({"event": "ws_stale_halt", "coin": self.coin, "gap_sec": gap}))
                # rate-limit resubscribe attempts
                if now - self._last_resubscribe < backoff:
                    continue
                jitter = random.uniform(0, backoff * 0.1)
                await asyncio.sleep(jitter)
                self._resubscribe()
                self._last_resubscribe = time.time()
                log.info(json.dumps({"event": "ws_resubscribe", "coin": self.coin, "backoff": backoff}))
                backoff = min(self._backoff_max, backoff * 2)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error(json.dumps({"event": "ws_watchdog_error", "coin": self.coin, "err": str(exc)}))
                backoff = min(self._backoff_max, backoff * 2)

    def _resubscribe(self) -> None:
        """
        Best-effort resubscribe to user fills and mids.
        """
        try:
            if self.info.ws_manager is None:
                return
            # attempt to unsubscribe old subs to avoid duplicates
            for key, sub_id in list(self._subs.items()):
                try:
                    self.info.unsubscribe({"type": "noop"}, sub_id)  # noop subscription object; unsubscribe ignores content
                except Exception:
                    pass
            self._subs.clear()
            # re-register callbacks
            # reuse the existing callbacks on this instance
            def _on_user_fills(msg: Any) -> None:
                try:
                    data = msg.get("data", {})
                    if data.get("user", "").lower() != self.account.lower():
                        return
                    fills = data.get("fills") or []
                    for f in fills:
                        if f.get("coin") != self.coin:
                            continue
                        # Skip fills older than min_fill_time_ms to avoid processing stale WS snapshot
                        fill_ts = int(f.get("time", 0))
                        if self.min_fill_time_ms > 0 and fill_ts < self.min_fill_time_ms:
                            continue
                        key = self._fill_key(f)
                        if not self._fill_seen.add(key):
                            continue
                        self.last_event = time.time()
                        if self._halted:
                            self._halted = False
                            logging.getLogger("gridbot").info(json.dumps({"event": "ws_resume", "coin": self.coin}))
                        if self.loop:
                            asyncio.run_coroutine_threadsafe(self.fill_queue.put(f), self.loop)
                except Exception:
                    pass

            def _on_ticker(msg: Any) -> None:
                try:
                    # Mark WS as alive on ANY message
                    self._last_ws_any_msg = time.time()
                    
                    data = msg.get("data", {})
                    mids = data.get("allMids", {})
                    mid = mids.get(self.coin)
                    if mid is None:
                        return
                    self._last_mid = float(mid)
                    self._last_mid_ts = time.time()
                    self.last_event = self._last_mid_ts
                    if self._halted:
                        self._halted = False
                        logging.getLogger("gridbot").info(json.dumps({"event": "ws_resume", "coin": self.coin}))
                except Exception:
                    pass

            # Per Hyperliquid docs: userFills has no dex param, allMids uses "dex" key
            sub_fills = self.info.subscribe({"type": "userFills", "user": self.account}, _on_user_fills)
            sub_mids = self.info.subscribe({"type": "allMids", "dex": self.dex}, _on_ticker)
            self._subs = {"fills": sub_fills, "mids": sub_mids}
            
            # C-3 FIX: Trigger REST poll after WS reconnect to catch fills missed during gap
            if self._on_reconnect and self.loop:
                try:
                    asyncio.run_coroutine_threadsafe(self._on_reconnect(), self.loop)
                    logging.getLogger("gridbot").info(
                        json.dumps({"event": "ws_reconnect_rest_poll_triggered", "coin": self.coin})
                    )
                except Exception as e:
                    logging.getLogger("gridbot").warning(
                        json.dumps({"event": "ws_reconnect_callback_error", "coin": self.coin, "err": str(e)})
                    )
        except Exception:
            pass

    def data_age(self) -> float:
        return time.time() - self.last_event

    def is_halted(self) -> bool:
        return self._halted
