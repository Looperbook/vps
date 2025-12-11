"""
MetaLoader: Metadata loading for exchange assets.

Extracts the complex metadata loading logic from bot.py to centralize:
- Asset metadata loading (tick size, decimals)
- CCXT market data integration
- Fallback logic for builder perps

Usage:
    loader = MetaLoader(info, async_info, coin, cfg.dex, log_event)
    result = await loader.load()
    # result.tick_sz, result.px_decimals, result.sz_decimals
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass
class MetaResult:
    """Result of metadata loading."""
    tick_sz: float
    px_decimals: int
    sz_decimals: int


class MetaLoader:
    """
    Loader for exchange asset metadata.
    
    Handles the complex logic of determining tick size and decimals
    from multiple sources:
    1. Exchange metadata (universe)
    2. CCXT market data
    3. Fallback inference from asset contexts
    4. Builder perp heuristics
    """
    
    def __init__(
        self,
        info: Any,
        async_info: Any,
        coin: str,
        dex: str,
        http_timeout: float = 5.0,
        log_event: Optional[Callable[..., None]] = None,
    ) -> None:
        """
        Initialize MetaLoader.
        
        Args:
            info: Hyperliquid Info API (sync)
            async_info: Hyperliquid AsyncInfo API
            coin: Coin symbol (e.g., "BTC" or "xyz:ASSET")
            dex: DEX type (e.g., "hl" or "xyz")
            http_timeout: HTTP timeout for API calls
            log_event: Logging callback
        """
        self.info = info
        self.async_info = async_info
        self.coin = coin
        self.dex = dex
        self.http_timeout = http_timeout
        self._log_event = log_event or (lambda *args, **kwargs: None)
        
        # Defaults
        self._default_px_decimals = 2
        self._default_sz_decimals = 3
        self._default_tick_sz = 0.01
    
    def _is_builder_asset(self) -> bool:
        """Check if this is a builder/xyz asset."""
        return self.dex.lower() == "xyz" or self.coin.startswith("xyz:")
    
    def _match_name(self, name: str) -> bool:
        """Check if asset name matches our coin."""
        if name == self.coin:
            return True
        short = self.coin.split(":")[-1]
        return name == short or name == f"{short}-PERP"
    
    async def _call_with_retry(self, fn: Callable, label: str, retries: int = 1) -> Any:
        """Run a blocking call in a thread with timeout."""
        loop = asyncio.get_running_loop()
        for attempt in range(retries + 1):
            try:
                return await asyncio.wait_for(
                    loop.run_in_executor(None, fn),
                    timeout=self.http_timeout
                )
            except asyncio.TimeoutError:
                if attempt == retries:
                    raise
                await asyncio.sleep(0.5)
            except Exception:
                if attempt == retries:
                    raise
                await asyncio.sleep(0.5)
    
    async def _load_universe_meta(self) -> tuple[List[Dict], float, int]:
        """Load metadata from exchange universe."""
        tick_sz = 0.0
        sz_decimals = self._default_sz_decimals
        
        # Load metadata
        builder_asset = self._is_builder_asset()
        
        if self.async_info and not builder_asset:
            try:
                meta = await self.async_info.meta(self.dex)
            except Exception as exc:
                self._log_event("meta_async_error", err=str(exc))
                meta = await self._call_with_retry(
                    lambda: self.info.meta(dex=self.dex), "meta"
                )
        else:
            meta = await self._call_with_retry(
                lambda: self.info.meta(dex=self.dex), "meta"
            )
        
        universe = meta.get("universe", [])
        
        # Find our asset in universe
        asset = next(
            (a for a in universe if self._match_name(a.get("name", ""))),
            None
        )
        
        if asset:
            sz_decimals = int(asset.get("szDecimals", sz_decimals))
            try:
                tick_val = float(asset.get("tickSz", 0.0))
                if tick_val > 0:
                    tick_sz = tick_val
            except Exception:
                pass
        
        return universe, tick_sz, sz_decimals
    
    async def _load_ccxt_tick(self, sz_decimals: int) -> tuple[float, int]:
        """Try to load tick size from CCXT market metadata."""
        tick_sz = 0.0
        
        if self._is_builder_asset():
            return tick_sz, sz_decimals
        
        try:
            import hyperliquid.ccxt.hyperliquid as ccxt_hl
            
            cc = ccxt_hl.hyperliquid()
            markets = cc.load_markets()
            short = self.coin.split(":")[-1]
            symbols_to_try = [f"{short}/USDC:USDC", short, self.coin]
            
            for sym in symbols_to_try:
                if sym in markets:
                    m = markets[sym]
                    price_inc = m.get("precision", {}).get("price")
                    amount_inc = m.get("precision", {}).get("amount")
                    
                    if price_inc and tick_sz == 0.0:
                        tick_sz = float(price_inc)
                    
                    if amount_inc and amount_inc < 1:
                        try:
                            sz_decimals = max(
                                sz_decimals,
                                int(round(-math.log10(amount_inc)))
                            )
                        except Exception:
                            pass
                    break
        except Exception:
            pass
        
        return tick_sz, sz_decimals
    
    async def _load_fallback_tick(
        self,
        universe: List[Dict],
        tick_candidates: List[float],
    ) -> List[float]:
        """Load fallback tick size from asset contexts."""
        if self._is_builder_asset():
            return tick_candidates
        
        try:
            ctxs = await asyncio.get_running_loop().run_in_executor(
                None, self.info.meta_and_asset_ctxs
            )
            asset_ctxs = ctxs[1] if isinstance(ctxs, list) and len(ctxs) > 1 else []
            
            for idx, ctx in enumerate(asset_ctxs):
                coin_name = universe[idx].get("name") if idx < len(universe) else None
                
                if coin_name and self._match_name(coin_name):
                    impact = ctx.get("impactPxs") or []
                    
                    if isinstance(impact, list) and len(impact) == 2:
                        try:
                            diff = abs(float(impact[1]) - float(impact[0]))
                            if diff > 0:
                                # Normalize to reasonable tick
                                for nice in (1.0, 0.5, 0.25, 0.1, 0.05, 0.01, 0.001, 0.0001):
                                    if diff >= nice:
                                        tick_candidates.append(nice)
                                        break
                                else:
                                    tick_candidates.append(diff)
                        except Exception:
                            pass
                    break
        except Exception:
            pass
        
        return tick_candidates
    
    async def _get_mid_price(self) -> float:
        """Get current mid price for the coin."""
        try:
            if self.async_info and not self._is_builder_asset():
                mids = await self.async_info.all_mids(self.dex)
            else:
                mids = self.info.all_mids(dex=self.dex)
            return float(mids.get(self.coin, 0.0))
        except Exception:
            return 0.0
    
    def _apply_builder_heuristics(
        self,
        tick_sz: float,
        mid_val: float,
    ) -> float:
        """Apply builder perp heuristics for tick size."""
        if not self._is_builder_asset():
            return tick_sz
        
        if mid_val >= 1000:
            tick_sz = max(tick_sz, 1.0)
        elif mid_val >= 10:
            tick_sz = max(tick_sz, 0.01)
        elif mid_val > 0:
            tick_sz = max(tick_sz, 0.001)
        
        # Avoid extremely small ticks
        if tick_sz < 1e-4:
            tick_sz = 0.0
        
        return tick_sz
    
    def _compute_px_decimals(
        self,
        tick_sz: float,
        sz_decimals: int,
        mid_val: float,
    ) -> int:
        """Compute price decimals from tick size."""
        if tick_sz > 0:
            return min(self._tick_to_decimals(tick_sz), 6)
        
        # Fallback to Hyperliquid rounding rule
        max_decimals = max(0, 6 - sz_decimals)
        
        if mid_val > 0:
            try:
                sig_step = 10 ** (int(math.floor(math.log10(mid_val))) - 4)
                max_decimals = min(max_decimals, self._tick_to_decimals(sig_step))
            except Exception:
                pass
        
        return min(6, max_decimals)
    
    @staticmethod
    def _tick_to_decimals(tick: float) -> int:
        """Convert tick size to decimal places."""
        if tick <= 0:
            return 0
        try:
            return max(0, -int(math.floor(math.log10(tick) + 1e-9)))
        except Exception:
            return 0
    
    async def load(self) -> MetaResult:
        """
        Load all metadata for the coin.
        
        Returns:
            MetaResult with tick_sz, px_decimals, sz_decimals
        """
        # Step 1: Load universe metadata
        universe, tick_primary, sz_decimals = await self._load_universe_meta()
        
        # Step 2: Try CCXT for tick size
        tick_ccxt, sz_decimals = await self._load_ccxt_tick(sz_decimals)
        if tick_ccxt > 0 and tick_primary == 0.0:
            tick_primary = tick_ccxt
        
        # Step 3: Fallback inference from asset contexts
        tick_candidates: List[float] = []
        if tick_primary == 0.0:
            tick_candidates = await self._load_fallback_tick(universe, tick_candidates)
        
        # Step 4: Select final tick size
        if tick_primary > 0:
            tick_sz = tick_primary
        elif tick_candidates:
            tick_candidates = sorted([t for t in tick_candidates if t > 0])
            tick_sz = tick_candidates[0] if tick_candidates else 0.0
        else:
            tick_sz = 0.0
        
        # Step 5: Get mid price for heuristics
        mid_val = await self._get_mid_price()
        
        # Step 6: Apply builder perp heuristics
        tick_sz = self._apply_builder_heuristics(tick_sz, mid_val)
        
        # Step 7: Compute price decimals
        px_decimals = self._compute_px_decimals(tick_sz, sz_decimals, mid_val)
        
        self._log_event(
            "meta_loaded",
            tick_sz=tick_sz,
            px_decimals=px_decimals,
            sz_decimals=sz_decimals,
        )
        
        return MetaResult(
            tick_sz=tick_sz,
            px_decimals=px_decimals,
            sz_decimals=sz_decimals,
        )
