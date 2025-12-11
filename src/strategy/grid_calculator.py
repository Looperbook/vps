"""
GridCalculator - Centralized grid computation module.

Extracts the pure calculation logic for building grids from the GridBot class.
This module handles:
- Grid level computation and spacing
- Price quantization to tick boundaries
- Risk filtering of grid levels
- Diff calculation between desired and existing orders
- Size scaling based on inventory skew

This is a pure calculation module with no side effects - all state changes
and order placement remain in the bot.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from src.strategy.strategy import GridLevel
from src.core.utils import quantize

if TYPE_CHECKING:
    from src.strategy.strategy import GridStrategy
    from src.risk.risk import RiskEngine as RiskManager
    from src.config.config import Settings


@dataclass
class GridDiff:
    """Result of grid diff calculation."""
    to_cancel_keys: List[str]  # Price keys to cancel
    to_place: List[GridLevel]  # Levels to place
    desired: Dict[str, GridLevel]  # Final desired levels (may include unchanged existing)
    

@dataclass
class GridBuildResult:
    """Result of grid build calculation."""
    levels: List[GridLevel]  # Risk-filtered levels
    spacing: float  # Computed spacing
    per_order_size: float  # Base size per order
    skipped_count: int  # Number of levels skipped by risk


class GridCalculator:
    """
    Centralized grid calculation logic.
    
    Provides pure functions for:
    - Building grid levels from strategy
    - Applying risk filters
    - Computing grid diffs
    - Calculating replacement levels after fills
    
    Thread-safety: All methods are stateless and thread-safe.
    This class holds configuration but no mutable state.
    """
    
    def __init__(
        self,
        cfg: "Settings",
        tick_sz: float,
        px_decimals: int,
        sz_decimals: int,
        effective_grids: int,
        effective_investment_usd: float
    ) -> None:
        """
        Initialize GridCalculator.
        
        Args:
            cfg: Bot settings
            tick_sz: Minimum price tick
            px_decimals: Decimal places for price
            sz_decimals: Decimal places for size
            effective_grids: Number of grid levels per side
            effective_investment_usd: Investment amount in USD
        """
        self.cfg = cfg
        self.tick_sz = tick_sz
        self.px_decimals = px_decimals
        self.sz_decimals = sz_decimals
        self.effective_grids = effective_grids
        self.effective_investment_usd = effective_investment_usd
    
    def price_key(self, side: str, px: float) -> str:
        """
        Generate deterministic price key using Decimal for precision.
        
        Uses Decimal to avoid floating-point representation issues
        that could cause duplicate keys or missed matches.
        
        Args:
            side: Order side ('buy' or 'sell')
            px: Price level
            
        Returns:
            Unique key string like "buy:100.50"
        """
        # Use Decimal for consistent string representation
        px_decimal = Decimal(str(px)).quantize(Decimal(10) ** -self.px_decimals)
        return f"{side}:{px_decimal}"
    
    def calculate_order_size(self, mid: float) -> float:
        """
        Calculate base per-order size based on investment and grid count.
        
        Args:
            mid: Current mid price
            
        Returns:
            Base order size (pre-scaling)
        """
        per_order_notional = (self.effective_investment_usd * self.cfg.leverage) / (2 * self.effective_grids)
        sz = math.floor((per_order_notional / max(mid, 1e-9)) * (10 ** self.sz_decimals)) / (10 ** self.sz_decimals)
        return sz
    
    def build_filtered_levels(
        self,
        strategy: "GridStrategy",
        risk: "RiskManager",
        mid: float,
        position: float,
        base_size: float,
        flatten_mode: bool = False
    ) -> GridBuildResult:
        """
        Build grid levels with risk filtering applied.
        
        Args:
            strategy: Grid strategy for level computation
            risk: Risk manager for order filtering
            mid: Current mid price
            position: Current position size
            base_size: Base order size
            flatten_mode: If True, only place reducing orders
            
        Returns:
            GridBuildResult with filtered levels and metadata
        """
        spacing = strategy.compute_spacing(mid, position=position, grid_center=strategy.grid_center)
        base_levels = strategy.build_grid(mid, position=position)
        
        # In flatten mode, only place orders that reduce exposure
        if flatten_mode:
            reduce_side = "sell" if position > 0 else "buy"
            base_levels = [lvl for lvl in base_levels if lvl.side == reduce_side]
        
        levels: List[GridLevel] = []
        sim_pos = position
        skipped = 0
        
        for lvl in base_levels:
            # Quantize price to tick boundaries
            lvl.px = quantize(lvl.px, self.tick_sz, lvl.side, self.px_decimals)
            
            # Scale size based on inventory skew
            scaled_sz = strategy.size_scale(lvl.side, position, mid, base_size)
            lvl.sz = scaled_sz
            
            # Risk check with simulated position
            if not risk.allow_order(lvl.side, lvl.sz, lvl.px, position_override=sim_pos):
                skipped += 1
                continue
            
            # Update simulated position for subsequent risk checks
            sim_pos = sim_pos + (lvl.sz if lvl.side == "buy" else -lvl.sz)
            levels.append(lvl)
        
        return GridBuildResult(
            levels=levels,
            spacing=spacing,
            per_order_size=base_size,
            skipped_count=skipped
        )
    
    def compute_grid_diff(
        self,
        desired_levels: List[GridLevel],
        existing_keys: Set[str],
        orders_by_price: Dict[str, any],
        reprice_tick_threshold: int
    ) -> GridDiff:
        """
        Compute what orders need to be canceled/placed.
        
        Uses smart diffing to:
        - Preserve queue position for unchanged orders
        - Only reprice if movement exceeds threshold
        - Minimize order churn
        
        Args:
            desired_levels: Target grid levels
            existing_keys: Price keys of existing orders
            orders_by_price: Current order records by price key
            reprice_tick_threshold: Tick threshold for repricing
            
        Returns:
            GridDiff with cancel and place lists
        """
        desired = {self._level_key(lvl): lvl for lvl in desired_levels}
        
        to_cancel_keys: List[str] = []
        to_place: List[GridLevel] = []
        
        # Keys that are no longer desired
        for key in existing_keys:
            if key not in desired:
                to_cancel_keys.append(key)
        
        # For existing keys, decide whether to reprice or leave
        for key in existing_keys & desired.keys():
            rec = orders_by_price.get(key)
            lvl = desired[key]
            
            if not rec:
                to_place.append(lvl)
                continue
            
            # If price moved more than threshold ticks, cancel/replace
            tick_threshold = max(0, reprice_tick_threshold)
            tick_move = abs(lvl.px - rec.level.px) / max(self.tick_sz, 1e-9)
            size_change = abs(lvl.sz - rec.level.sz) / max(rec.level.sz, 1e-9) if rec.level.sz else 1.0
            
            if tick_threshold > 0 and tick_move <= tick_threshold and size_change < 0.2:
                # Keep existing to preserve queue; no action
                desired[key] = rec.level  # Keep reference
                continue
            
            to_cancel_keys.append(key)
        
        # Place missing (not in existing or marked for cancel)
        existing_after_cancel = existing_keys - set(to_cancel_keys)
        for key, lvl in desired.items():
            if key not in existing_after_cancel:
                to_place.append(lvl)
        
        return GridDiff(
            to_cancel_keys=to_cancel_keys,
            to_place=to_place,
            desired=desired
        )
    
    def calculate_replacement_level(
        self,
        strategy: "GridStrategy",
        fill_side: str,
        fill_px: float,
        fill_sz: float,
        position: float,
        flatten_mode: bool = False
    ) -> Optional[GridLevel]:
        """
        Calculate replacement level after a fill.
        
        Args:
            strategy: Grid strategy
            fill_side: Side of the filled order
            fill_px: Price of the fill
            fill_sz: Size of the fill
            position: Current position after fill
            flatten_mode: If True, skip orders that increase exposure
            
        Returns:
            Replacement GridLevel or None if should be skipped
        """
        spacing = strategy.compute_spacing(fill_px, position=position, grid_center=strategy.grid_center)
        target_side = "sell" if fill_side.startswith("b") else "buy"
        target_px = fill_px * (1 + spacing) if target_side == "sell" else fill_px * (1 - spacing)
        
        jitter = 1 + random.uniform(-self.cfg.random_size_jitter, self.cfg.random_size_jitter)
        sz_base = fill_sz * jitter
        sz_scaled = strategy.size_scale(target_side, position, fill_px, sz_base)
        sz = math.floor(sz_scaled * (10 ** self.sz_decimals)) / (10 ** self.sz_decimals)
        
        lvl = GridLevel(target_side, target_px, sz)
        
        # In flatten mode, disallow orders that increase exposure
        if flatten_mode:
            disallowed = "buy" if position > 0 else "sell"
            if lvl.side == disallowed:
                return None
        
        return lvl
    
    def quantize_level(self, lvl: GridLevel) -> GridLevel:
        """
        Quantize a grid level's price to tick boundaries.
        
        Args:
            lvl: Grid level to quantize
            
        Returns:
            Level with quantized price (modifies in place and returns)
        """
        lvl.px = quantize(lvl.px, self.tick_sz, lvl.side, self.px_decimals)
        return lvl
    
    def _level_key(self, lvl: GridLevel) -> str:
        """Generate price key for a grid level."""
        return self.price_key(lvl.side, lvl.px)
    
    def get_state(self) -> Dict:
        """
        Get calculator state for persistence.
        
        Returns:
            Dictionary with configuration values
        """
        return {
            "tick_sz": self.tick_sz,
            "px_decimals": self.px_decimals,
            "sz_decimals": self.sz_decimals,
            "effective_grids": self.effective_grids,
            "effective_investment_usd": str(self.effective_investment_usd)
        }
    
    def update_config(
        self,
        effective_grids: Optional[int] = None,
        effective_investment_usd: Optional[float] = None
    ) -> None:
        """
        Update calculator configuration.
        
        Used when config changes dynamically (e.g., per-coin overrides).
        
        Args:
            effective_grids: New grid count (optional)
            effective_investment_usd: New investment amount (optional)
        """
        if effective_grids is not None:
            self.effective_grids = effective_grids
        if effective_investment_usd is not None:
            self.effective_investment_usd = effective_investment_usd
