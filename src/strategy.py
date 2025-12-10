"""
Grid strategy with ATR/EWMA spacing, trend bias, and inventory skew.
"""

from __future__ import annotations

import logging
import json
import math
from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple
import random

from src.config import Settings
from src.utils import tick_to_decimals


@dataclass
class GridLevel:
    side: str
    px: float
    sz: float
    oid: Optional[int] = None
    cloid: Optional[str] = None


class VolatilityModel:
    def __init__(self, atr_len: int, ewma_alpha: float, init_vol_multiplier: float = 1.0) -> None:
        self.atr_len = atr_len
        self.ewma_alpha = ewma_alpha
        self.init_vol_multiplier = init_vol_multiplier
        self.window: Deque[float] = deque(maxlen=atr_len)
        self.prev: Optional[float] = None
        self.ewma_var: float = 0.0

    def step(self, price: float) -> Tuple[float, float]:
        if self.prev is None:
            self.prev = price
            base_seed = price * 0.001 * self.init_vol_multiplier
            seed = max(base_seed, self.atr_len * 0.0001)
            self.window.append(seed)
            # Seed EWMA variance to avoid starting at zero volatility.
            base_vol = 0.001 * self.init_vol_multiplier
            self.ewma_var = base_vol * base_vol
            return seed, base_vol
        tr = abs(price - self.prev)
        self.prev = price
        self.window.append(tr)
        atr = sum(self.window) / len(self.window) if self.window else 0.0
        ret = tr / max(price, 1e-9)
        self.ewma_var = self.ewma_alpha * (ret ** 2) + (1 - self.ewma_alpha) * self.ewma_var
        vol = math.sqrt(self.ewma_var) if self.ewma_var > 0 else 0.0
        return atr, vol


class TrendModel:
    def __init__(self, fast: int, slow: int) -> None:
        self.fast = fast
        self.slow = slow
        self.fast_val: Optional[float] = None
        self.slow_val: Optional[float] = None

    def update(self, price: float) -> float:
        self.fast_val = self._ema(self.fast_val, price, self.fast)
        self.slow_val = self._ema(self.slow_val, price, self.slow)
        if self.fast_val is None or self.slow_val is None:
            return 0.0
        return self.fast_val - self.slow_val

    @staticmethod
    def _ema(prev: Optional[float], value: float, length: int) -> float:
        if prev is None:
            return value
        alpha = 2 / (length + 1)
        return alpha * value + (1 - alpha) * prev


class GridStrategy:
    def __init__(self, cfg: Settings, tick_sz: float, px_decimals: int, sz_decimals: int, log_sample: float = 0.0, per_coin_cfg: Optional[dict] = None) -> None:
        self.cfg = cfg
        self.per_coin_cfg = per_coin_cfg or {}
        self.vol_model = VolatilityModel(cfg.atr_len, cfg.ewma_alpha, cfg.init_vol_multiplier)
        self.trend_model = TrendModel(cfg.trend_fast_ema, cfg.trend_slow_ema)
        self.grid_center: Optional[float] = None
        self.px_decimals = px_decimals
        self.sz_decimals = sz_decimals
        self.tick_sz = tick_sz
        self.last_atr: float = 0.0
        self.last_vol: float = 0.0
        self.last_trend_sign: int = 0
        self.log_sample = max(0.0, min(1.0, log_sample))

    def _get_config_value(self, key: str, default=None):
        """Get config value with per-coin override support."""
        if isinstance(self.per_coin_cfg, dict) and key in self.per_coin_cfg:
            return self.per_coin_cfg[key]
        return getattr(self.cfg, key, default)

    @property
    def effective_grids(self) -> int:
        return int(self._get_config_value("grids", self.cfg.grids))

    @property
    def effective_base_spacing_pct(self) -> float:
        return float(self._get_config_value("base_spacing_pct", self.cfg.base_spacing_pct))

    @property
    def effective_investment_usd(self) -> float:
        return float(self._get_config_value("investment_usd", self.cfg.investment_usd))

    def on_price(self, price: float) -> Tuple[float, float]:
        self.last_atr, self.last_vol = self.vol_model.step(price)
        if self._log():
            logging.getLogger("gridbot").info(
                json.dumps(
                    {
                        "event": "price_update",
                        "px": price,
                        "atr": self.last_atr,
                        "ewma_vol": self.last_vol,
                    }
                )
            )
        return self.last_atr, self.last_vol

    def compute_spacing(self, px: float, position: float = 0.0, grid_center: Optional[float] = None) -> float:
        atr_adj = self.last_atr / max(px, 1e-9)
        vol_adj = self.last_vol * 2.5
        raw = self.effective_base_spacing_pct + atr_adj * 0.5 + vol_adj
        
        # Critical-7: Minimum spacing must be at least 2 ticks to avoid overlap
        tick_floor = max(self.tick_sz / max(px, 1e-9) * 2, self.effective_base_spacing_pct)
        # Critical-7: Also enforce minimum spread to prevent self-trading (at least 10 bps)
        min_spread_spacing = max(0.001, tick_floor)
        
        # HARDENED: tighten spacing aggressively when in a losing position to deleverage faster.
        center = grid_center if grid_center is not None else px
        loss_drift = 0.0
        if position != 0:
            sign = 1 if position > 0 else -1
            # positive drift_pct means price moved against our position
            loss_drift = max(0.0, sign * (center - px) / max(px, 1e-9))
        if loss_drift > 0:
            # Critical-7: Limit tightening to 50% (floor at 50% of normal) to prevent collapse
            tighten_mult = max(0.50, 1 - min(0.50, loss_drift * 10))
        else:
            tighten_mult = 1.0
        
        # Apply tightening to raw spacing
        adjusted_spacing = raw * tighten_mult
        
        # Critical-7: Final floor check - spacing must be at least min_spread_spacing
        spacing = min(self.cfg.max_spacing_pct, max(min_spread_spacing, adjusted_spacing))
        
        # Critical-7: Validate adjacent levels won't cross (spacing * grids must give reasonable spread)
        min_total_spread = min_spread_spacing * 2  # At least 2x minimum on each side
        if spacing < min_total_spread / self.effective_grids:
            old_spacing = spacing
            spacing = min_total_spread / self.effective_grids
            if self._log():
                logging.getLogger("gridbot").warning(
                    json.dumps({
                        "event": "spacing_compression_prevented",
                        "old_spacing": old_spacing,
                        "new_spacing": spacing,
                        "min_required": min_total_spread / self.effective_grids
                    })
                )
        
        if self._log():
            logging.getLogger("gridbot").info(
                json.dumps(
                    {
                        "event": "spacing_compute",
                        "px": px,
                        "atr_adj": atr_adj,
                        "vol_adj": vol_adj,
                        "raw_spacing": raw,
                        "spacing": spacing,
                        "loss_drift": loss_drift,
                        "tighten_mult": tighten_mult,
                    }
                )
            )
        return spacing

    def trend_bias(self, px: float) -> float:
        delta = self.trend_model.update(px)
        bias = max(min(delta / max(px, 1e-9), 0.004), -0.004)
        sign = 1 if bias > 0 else -1 if bias < 0 else 0
        if sign != self.last_trend_sign:
            logging.getLogger("gridbot").info(
                json.dumps({"event": "trend_flip", "prev_sign": self.last_trend_sign, "new_sign": sign, "bias": bias})
            )
            self.last_trend_sign = sign
        if self._log():
            logging.getLogger("gridbot").info(json.dumps({"event": "trend_bias", "px": px, "delta": delta, "bias": bias}))
        return bias

    def build_grid(self, mid: float, position: float = 0.0) -> List[GridLevel]:
        spacing = self.compute_spacing(mid, position=position, grid_center=self.grid_center)
        bias = self.trend_bias(mid)
        levels: List[GridLevel] = []
        for i in range(1, self.effective_grids + 1):
            offset = spacing * i
            up_px = round(mid * (1 + offset + bias), self.px_decimals)
            dn_px = round(mid * (1 - offset - bias), self.px_decimals)
            levels.append(GridLevel("sell", up_px, 0.0))
            levels.append(GridLevel("buy", dn_px, 0.0))
        self.grid_center = mid
        if self._log():
            logging.getLogger("gridbot").info(
                json.dumps(
                    {
                        "event": "grid_built",
                        "mid": mid,
                        "spacing": spacing,
                        "bias": bias,
                        "levels": [{"side": l.side, "px": l.px, "sz": l.sz} for l in levels],
                    }
                )
            )
        return levels

    def size_scale(self, side: str, position: float, mid: float, base_sz: float) -> float:
        target_notional = self.effective_investment_usd * self.cfg.leverage
        target_pos = target_notional / max(mid, 1e-9)
        ratio = abs(position) / max(target_pos, 1e-9)
        max_scale = 1.2
        min_scale = 0.3
        if ratio <= self.cfg.skew_soft:
            scaled = base_sz
        else:
            if position > 0:
                if side == "buy":
                    scaled = base_sz * max(min_scale, 1 - (ratio - self.cfg.skew_soft))
                else:
                    scaled = base_sz * min(max_scale, 1 + (ratio - self.cfg.skew_soft) * 0.5)
            elif position < 0:
                if side == "sell":
                    scaled = base_sz * max(min_scale, 1 - (ratio - self.cfg.skew_soft))
                else:
                    scaled = base_sz * min(max_scale, 1 + (ratio - self.cfg.skew_soft) * 0.5)
            else:
                scaled = base_sz
        # cap absolute order size to 10% of target position to avoid blowups
        cap = max(target_pos * 0.1, base_sz)
        scaled = max(min_scale * base_sz, min(scaled, cap))
        return scaled

    def _log(self) -> bool:
        if self.log_sample >= 1.0:
            return True
        if self.log_sample <= 0.0:
            return False
        return random.random() < self.log_sample
