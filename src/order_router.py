"""
Nonce-safe order router serializing submissions and cancels.
"""

from __future__ import annotations

import asyncio
import json
import time
import secrets
from dataclasses import dataclass
from typing import Any, Optional
from collections import deque
from statistics import median

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.types import Cloid

from src.config import Settings
from src.utils import quantize, is_divisible, tick_to_decimals
from src.rounding import round_price, snap_to_tick
import logging
import random


@dataclass
class OrderRequest:
    is_buy: bool
    sz: float
    px: float
    reduce_only: bool = False
    cloid: Optional[str] = None

    def to_kwargs(self) -> dict[str, Any]:
        return {
            "is_buy": self.is_buy,
            "sz": self.sz,
            "px": self.px,
            "reduce_only": self.reduce_only,
            "cloid": self.cloid,
        }


class OrderRouter:
    def __init__(
        self,
        coin: str,
        exchange,
        info: Info,
        cfg: Settings,
        px_decimals: int,
        tick_sz: float,
        sz_decimals: int,
        nonce_lock: asyncio.Lock,
    ) -> None:
        self.coin = coin
        self.exchange = exchange
        self.info = info
        self.cfg = cfg
        self.px_decimals = px_decimals
        self.tick_sz = tick_sz
        self.sz_decimals = sz_decimals
        # If tick is unknown/zero (builder perps often omit it), fall back to px_decimals for rounding.
        self.tick_decimals = tick_to_decimals(self.tick_sz) if self.tick_sz > 0 else self.px_decimals
        self.nonce_lock = nonce_lock
        self._http_timeout = cfg.http_timeout
        self.queue: asyncio.Queue[tuple[OrderRequest | None, asyncio.Future | None]] = asyncio.Queue()
        self._worker: Optional[asyncio.Task] = None
        self.coalesce_ms = self.cfg.coalesce_ms
        self.use_cloid = self.cfg.use_cloid
        self._cloid_counter = 0
        self._stop_sentinel = object()
        self._stopping = False
        # Hyperliquid order actions behave more reliably with modest bulk sizes.
        # Keep batches small to avoid silent failures / missing statuses from the API.
        self.max_bulk = 20
        # Per-coin rate limiter to avoid bursts; simple token bucket.
        self._tokens = 5
        self._last_refill = time.time()
        self._refill_rate = 5  # tokens per second
        # adaptive coalescing state
        # adaptive coalescing state (percentile-based)
        self._latencies: deque[float] = deque(maxlen=getattr(self.cfg, "coalesce_window", 50))
        self.coalesce_pct = getattr(self.cfg, "coalesce_pct", 0.9)
        self.coalesce_min_ms = max(1, int(self.cfg.coalesce_ms / 4))
        self.coalesce_max_ms = max(self.cfg.coalesce_ms, 200)

    def start(self) -> None:
        if self._worker is None:
            self._worker = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopping = True
        if self._worker:
            try:
                await self.queue.put((None, None))
            except Exception:
                pass
            try:
                while not self.queue.empty():
                    req, fut = self.queue.get_nowait()
                    if fut and not fut.done():
                        fut.set_exception(RuntimeError("router_stopping"))
            except Exception:
                pass
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)
            self._worker = None

    async def submit(self, req: OrderRequest) -> Optional[dict[str, Any]]:
        if self._stopping:
            raise RuntimeError("router_stopping")
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._throttle()
        await self.queue.put((req, fut))
        # Sample router_enqueue to reduce spam - use DEBUG level since it's high-frequency
        sample = getattr(self.cfg, "log_submit_sample", 0.05)
        if random.random() < sample:
            logging.getLogger("gridbot").debug(json.dumps({"event": "router_enqueue", "coin": self.coin, "side": "buy" if req.is_buy else "sell", "px": req.px, "sz": req.sz}))
        return await fut

    async def _run(self) -> None:
        while True:
            try:
                first_req, first_fut = await self.queue.get()
                if first_req is None and first_fut is None:
                    break
                batch = [(first_req, first_fut)]
                start = asyncio.get_event_loop().time()
                while True:
                    remaining = self.coalesce_ms / 1000 - (asyncio.get_event_loop().time() - start)
                    if remaining <= 0:
                        break
                    try:
                        req, fut = await asyncio.wait_for(self.queue.get(), timeout=remaining)
                        if req is None and fut is None:
                            # drain sentinel and stop after flushing what we have
                            break
                        batch.append((req, fut))
                        # Re-check if we've hit max batch size to avoid excessive delay (Bug #9)
                        if len(batch) >= self.max_bulk:
                            break
                    except asyncio.TimeoutError:
                        break
                await self._flush(batch)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logging.getLogger("gridbot").error(json.dumps({"event": "router_worker_error", "coin": self.coin, "err": str(exc)}))
                # do not drop outstanding futures silently
                for req, fut in locals().get("batch", []):
                    if fut and not fut.done():
                        fut.set_exception(exc)

    async def _flush(self, batch: list[tuple[OrderRequest, asyncio.Future]]) -> None:
        order_type = {"limit": {"tif": "Gtc"}}
        async with self.nonce_lock:
            if len(batch) > 1:
                for i in range(0, len(batch), self.max_bulk):
                    await self._flush_bulk(batch[i : i + self.max_bulk], order_type)
                return
            req, fut = batch[0]
            try:
                # Hyperliquid rounding: 5 sig figs, max (6 - szDecimals) decimals, then snap to tick and format
                rounded = round_price(req.px, self.sz_decimals, is_perp=True)
                px = snap_to_tick(rounded, self.tick_sz, "buy" if req.is_buy else "sell")
                px = round(px, self.tick_decimals)
                if not is_divisible(px, self.tick_sz):
                    fut.set_exception(RuntimeError("invalid_price_tick"))
                    logging.getLogger("gridbot").error(json.dumps({"event": "router_invalid_price", "coin": self.coin, "px": px, "tick": self.tick_sz}))
                    return
                if px * req.sz < self.cfg.min_notional:
                    fut.set_result(None)
                    logging.getLogger("gridbot").info(json.dumps({"event": "router_skip_notional", "coin": self.coin, "px": px, "sz": req.sz}))
                    return
                cloid = None
                if self.use_cloid:
                    # Reuse pre-generated cloid if available, otherwise generate new one
                    if req.cloid:
                        cloid = Cloid(req.cloid)
                    else:
                        self._cloid_counter += 1
                        cloid = Cloid(f"0x{secrets.token_hex(16)}")
                        req.cloid = str(cloid)
                sample = getattr(self.cfg, "log_submit_sample", 0.05)
                if random.random() < sample:
                    logging.getLogger("gridbot").debug(
                        json.dumps(
                            {
                                "event": "router_send",
                                "coin": self.coin,
                                "side": "buy" if req.is_buy else "sell",
                                "px": px,
                                "sz": req.sz,
                                "cloid": str(cloid) if cloid else None,
                                "ts": time.time(),
                            }
                        )
                    )
                # measure latency to adjust coalescing dynamically
                start_ts = asyncio.get_event_loop().time()
                resp = await self.exchange.order(
                    self.coin,
                    req.is_buy,
                    req.sz,
                    px,
                    order_type,
                    req.reduce_only,
                    cloid=cloid,
                )
                end_ts = asyncio.get_event_loop().time()
                lat_ms = max(0.0, (end_ts - start_ts) * 1000.0)
                # append to rolling window
                try:
                    self._latencies.append(lat_ms)
                    # compute percentile-based metric
                    if len(self._latencies) >= 3:
                        arr = sorted(self._latencies)
                        idx = int(min(len(arr) - 1, max(0, round(self.coalesce_pct * (len(arr) - 1)))))
                        pct_val = arr[idx]
                        # map percentile latency to coalescing window (scale factor 0.5)
                        target = int(min(self.coalesce_max_ms, max(self.coalesce_min_ms, pct_val * 0.5)))
                        self.coalesce_ms = target
                except Exception:
                    # fallback to previous value if something goes wrong
                    pass
                err_msg = self._extract_error_message(resp)
                if err_msg:
                    logging.getLogger("gridbot").error(json.dumps({"event": "router_error", "coin": self.coin, "err": err_msg}))
                    fut.set_exception(RuntimeError(err_msg))
                    return
                oid, cl = self._extract_ids(resp)
                fut.set_result({"resp": resp, "oid": oid, "cloid": cl or (str(cloid) if cloid else None)})
                self._log_submit(req.is_buy, px, req.sz, oid, cl or (str(cloid) if cloid else None))
            except Exception as exc:
                logging.getLogger("gridbot").error(json.dumps({"event": "router_error", "coin": self.coin, "err": str(exc)}))
                fut.set_exception(exc)

    async def _flush_bulk(self, batch: list[tuple[OrderRequest, asyncio.Future]], order_type: dict) -> None:
        from hyperliquid.utils import signing

        reqs = []
        idx_map = []
        cloids: list[Optional[str]] = []
        for idx, (req, fut) in enumerate(batch):
            rounded = round_price(req.px, self.sz_decimals, is_perp=True)
            px = snap_to_tick(rounded, self.tick_sz, "buy" if req.is_buy else "sell")
            px = round(px, self.tick_decimals)
            if not is_divisible(px, self.tick_sz):
                fut.set_exception(RuntimeError("invalid_price_tick"))
                logging.getLogger("gridbot").error(json.dumps({"event": "router_invalid_price", "coin": self.coin, "px": px, "tick": self.tick_sz}))
                continue
            if px * req.sz < self.cfg.min_notional:
                fut.set_result(None)
                logging.getLogger("gridbot").info(json.dumps({"event": "router_skip_notional", "coin": self.coin, "px": px, "sz": req.sz}))
                continue
            cloid = None
            if self.use_cloid:
                # Reuse pre-generated cloid if available, otherwise generate new one
                if req.cloid:
                    cloid = Cloid(req.cloid)
                else:
                    self._cloid_counter += 1
                    cloid = Cloid(f"0x{secrets.token_hex(16)}")
                    req.cloid = str(cloid)
                cloids.append(str(cloid))
            else:
                cloids.append(None)
            sample = getattr(self.cfg, "log_submit_sample", 0.05)
            if random.random() < sample:
                logging.getLogger("gridbot").info(
                    json.dumps(
                        {
                            "event": "router_send",
                            "coin": self.coin,
                            "side": "buy" if req.is_buy else "sell",
                            "px": px,
                            "sz": req.sz,
                            "cloid": str(cloid) if cloid else None,
                            "ts": time.time(),
                            "mode": "bulk",
                        }
                    )
                )
            reqs.append(
                signing.OrderRequest(
                    coin=self.coin,
                    is_buy=req.is_buy,
                    sz=req.sz,
                    limit_px=px,
                    order_type=order_type,
                    reduce_only=req.reduce_only,
                    cloid=cloid,
                )
            )
            idx_map.append(idx)
        if not reqs:
            return
        resp = await self.exchange.bulk_orders(reqs)
        log = logging.getLogger("gridbot")
        statuses = self._extract_statuses(resp)
        if not statuses:
            err_msg = self._extract_error_message(resp)
            if err_msg:
                log.error(json.dumps({"event": "router_error", "coin": self.coin, "err": err_msg, "status_count": len(statuses)}))
                for idx in idx_map:
                    _, fut = batch[idx]
                    fut.set_exception(RuntimeError(err_msg))
                return
            # No statuses returned; return cloid-only results so callers can still track orders
            for pos, idx in enumerate(idx_map):
                req, fut = batch[idx]
                cloid_val = cloids[pos] if pos < len(cloids) else req.cloid
                fut.set_result({"resp": resp, "oid": None, "cloid": cloid_val})
            log.info(json.dumps({"event": "router_missing_status", "coin": self.coin, "resp": resp}))
            return
        pos_by_idx = {idx: pos for pos, idx in enumerate(idx_map)}
        for pos, st in enumerate(statuses):
            i = idx_map[pos] if pos < len(idx_map) else None
            if i is None:
                continue
            req, fut = batch[i]
            try:
                if isinstance(st, dict) and st.get("error"):
                    log.error(json.dumps({"event": "router_error_status", "coin": self.coin, "err": st.get("error"), "px": req.px, "sz": req.sz}))
                    fut.set_exception(RuntimeError(str(st.get("error"))))
                    continue
                oid, cl = self._extract_ids(st)
                cloid_val = cl or (cloids[pos] if pos < len(cloids) else req.cloid)
                fut.set_result({"resp": resp, "oid": oid, "cloid": cloid_val})
                px = quantize(req.px, self.tick_sz, "buy" if req.is_buy else "sell", self.px_decimals)
                self._log_submit(req.is_buy, px, req.sz, oid, cloid_val)
            except Exception as exc:
                log.error(json.dumps({"event": "router_error", "coin": self.coin, "err": str(exc)}))
                fut.set_exception(exc)
        # Resolve any futures that didn't get a status (avoid hanging callers)
        for idx, (req, fut) in enumerate(batch):
            if fut.done():
                continue
            pos = pos_by_idx.get(idx)
            cloid_val = cloids[pos] if pos is not None and pos < len(cloids) else req.cloid
            fut.set_result({"resp": resp, "oid": None, "cloid": cloid_val})
            sample = getattr(self.cfg, "log_submit_sample", 0.05)
            if random.random() < sample:
                log.info(json.dumps({"event": "router_missing_status", "coin": self.coin, "px": req.px, "sz": req.sz}))

    def _extract_ids(self, resp: Any) -> tuple[Optional[int], Optional[str]]:
        """
        Extract oid/cloid from any response shape (single or bulk, resting or filled).
        Hyperliquid responses can look like:
        {"status":"ok","response":{"data":{"statuses":[{"resting":{"oid":123,"cloid":"0x.."}}]}}}
        or the status dict itself: {"resting":{...}} or {"filled":{...}}.
        """
        def parse_dict(d: dict) -> tuple[Optional[int], Optional[str]]:
            oid_val: Optional[int] = None
            cloid_val: Optional[str] = None
            if "oid" in d:
                try:
                    oid_val = int(d.get("oid"))  # type: ignore[arg-type]
                except Exception:
                    try:
                        oid_val = int(str(d.get("oid")), 0)
                    except Exception:
                        oid_val = None
            if "cloid" in d and d.get("cloid") is not None:
                cloid_val = str(d.get("cloid"))
            return oid_val, cloid_val

        try:
            if not isinstance(resp, dict):
                return None, None
            # unwrap full response if present
            if "response" in resp:
                resp = resp.get("response", {}).get("data", {}) or resp
            # statuses list
            if "statuses" in resp and isinstance(resp.get("statuses"), list):
                sts = resp.get("statuses") or []
                for st in sts:
                    if not isinstance(st, dict):
                        continue
                    for key in ("resting", "filled", "cancelled", "order"):
                        if isinstance(st.get(key), dict):
                            oid_val, cloid_val = parse_dict(st[key])
                            if oid_val is not None or cloid_val is not None:
                                return oid_val, cloid_val
                    oid_val, cloid_val = parse_dict(st)
                    if oid_val is not None or cloid_val is not None:
                        return oid_val, cloid_val
            # direct dict with resting/filled
            for key in ("resting", "filled", "cancelled", "order"):
                if isinstance(resp.get(key), dict):
                    oid_val, cloid_val = parse_dict(resp[key])
                    if oid_val is not None or cloid_val is not None:
                        return oid_val, cloid_val
            oid_val, cloid_val = parse_dict(resp)
            return oid_val, cloid_val
        except Exception:
            return None, None

    def _extract_statuses(self, resp: Any) -> list[Any]:
        if not isinstance(resp, dict):
            return []
        payload: Any = resp.get("response", resp)
        if isinstance(payload, dict) and "data" in payload:
            payload = payload.get("data", payload)
        statuses = payload.get("statuses") if isinstance(payload, dict) else None
        return statuses if isinstance(statuses, list) else []

    def _extract_error_message(self, resp: Any) -> Optional[str]:
        if not isinstance(resp, dict):
            return None
        if resp.get("status") == "err":
            return str(resp.get("response", resp))
        statuses = self._extract_statuses(resp)
        for st in statuses:
            if isinstance(st, dict) and st.get("error"):
                return str(st.get("error"))
        payload = resp.get("response", {}).get("data", {}) if isinstance(resp.get("response"), dict) else resp.get("response")
        if isinstance(payload, dict) and payload.get("error"):
            return str(payload.get("error"))
        return None

    def _log_submit(self, is_buy: bool, px: float, sz: float, oid: Optional[int], cloid: Optional[str]) -> None:
        import logging

        # Sample to limit log volume; always log if oid is None or on errors
        try:
            sample = float(self.cfg.log_submit_sample)
        except Exception:
            sample = 0.05
        if oid is not None and random.random() >= sample:
            return
        if random.random() < sample:
            logging.getLogger("gridbot").info(
                json.dumps(
                    {
                        "event": "order_submit",
                        "coin": self.coin,
                        "side": "buy" if is_buy else "sell",
                        "px": px,
                        "sz": sz,
                        "oid": oid,
                        "cloid": cloid,
                    }
                )
            )

    def _to_cloid(self, value: Optional[str | Cloid]) -> Optional[Cloid]:
        if value is None:
            return None
        if isinstance(value, Cloid):
            return value
        try:
            v = str(value)
            if not v.startswith("0x"):
                v = f"0x{v}"
            return Cloid(v)
        except Exception:
            return None

    async def safe_cancel(self, cloid: Optional[str | Cloid] = None, oid: Optional[int | str] = None) -> None:
        async with self.nonce_lock:
            first_err = None
            if cloid is not None:
                co = self._to_cloid(cloid)
                if co is not None:
                    try:
                        await self.exchange.cancel_by_cloid(self.coin, co)
                        logging.getLogger("gridbot").info(json.dumps({"event": "cancel", "coin": self.coin, "cloid": str(co)}))
                        return
                    except Exception as exc:
                        first_err = exc
            if oid is not None:
                try:
                    oid_int = int(oid)
                except Exception:
                    oid_int = None
                if oid_int is not None:
                    try:
                        await self.exchange.cancel(self.coin, oid_int)
                        logging.getLogger("gridbot").info(json.dumps({"event": "cancel", "coin": self.coin, "oid": oid_int}))
                        return
                    except Exception as exc:
                        first_err = first_err or exc
            if first_err:
                logging.getLogger("gridbot").error(
                    json.dumps({"event": "cancel_error", "coin": self.coin, "err": str(first_err), "cloid_type": str(type(cloid)), "oid_type": str(type(oid))})
                )

    async def cancel_all(self) -> None:
        """
        Cancel all open orders for this coin. Handles both async (AsyncExchange) and sync Exchange.
        """
        loop = asyncio.get_running_loop()
        # fetch open orders via Info (blocking) in executor
        open_orders = await loop.run_in_executor(None, lambda: self.info.frontend_open_orders(self.cfg.resolve_account(), dex=self.cfg.dex))
        cancels_cloid = []
        cancels_oid = []
        if isinstance(open_orders, dict):
            open_orders = open_orders.get("openOrders", [])
        if isinstance(open_orders, list):
            for o in open_orders:
                if o.get("coin") != self.coin:
                    continue
                coid = o.get("cloid")
                oid = o.get("oid")
                if coid:
                    from hyperliquid.utils import signing

                    co = self._to_cloid(coid)
                    if co is not None:
                        try:
                            cancels_cloid.append(signing.CancelByCloidRequest(coin=self.coin, cloid=co))
                        except Exception:
                            pass
                if oid is not None:
                    from hyperliquid.utils import signing

                    try:
                        cancels_oid.append(signing.CancelRequest(coin=self.coin, oid=int(oid)))
                    except Exception:
                        pass
        # detect whether exchange methods are coroutines (AsyncExchange) or sync
        async def _do_async():
            if cancels_cloid:
                await self.exchange.bulk_cancel_by_cloid(cancels_cloid)
            if cancels_oid:
                await self.exchange.bulk_cancel(cancels_oid)

        def _do_sync():
            if cancels_cloid:
                self.exchange.bulk_cancel_by_cloid(cancels_cloid)
            if cancels_oid:
                self.exchange.bulk_cancel(cancels_oid)

        try:
            if asyncio.iscoroutinefunction(getattr(self.exchange, "bulk_cancel_by_cloid", None)) or asyncio.iscoroutinefunction(getattr(self.exchange, "bulk_cancel", None)):
                await _do_async()
            else:
                await loop.run_in_executor(None, _do_sync)
        except Exception:
            # fallback best-effort per-order cancel
            for c in cancels_cloid:
                try:
                    if asyncio.iscoroutinefunction(getattr(self.exchange, "cancel_by_cloid", None)):
                        await self.exchange.cancel_by_cloid(self.coin, c.cloid)
                    else:
                        await loop.run_in_executor(None, lambda: self.exchange.cancel_by_cloid(self.coin, c.cloid))
                except Exception:
                    pass
            for c in cancels_oid:
                try:
                    if asyncio.iscoroutinefunction(getattr(self.exchange, "cancel", None)):
                        await self.exchange.cancel(self.coin, c.oid)
                    else:
                        await loop.run_in_executor(None, lambda: self.exchange.cancel(self.coin, c.oid))
                except Exception:
                    pass

    async def _throttle(self) -> None:
        """
        Simple token bucket to limit bursts per coin.
        """
        now = time.time()
        elapsed = max(0.0, now - self._last_refill)
        self._tokens = min(10, self._tokens + elapsed * self._refill_rate)
        self._last_refill = now
        if self._tokens >= 1:
            self._tokens -= 1
            return
        # wait for one token
        wait_for = max(0.0, (1 - self._tokens) / self._refill_rate)
        await asyncio.sleep(wait_for)
        self._tokens = max(0.0, self._tokens - 1)
