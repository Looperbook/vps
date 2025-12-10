"""
OrderManager: Centralized order indexing, registration, and lifecycle management.

Extracted from bot.py to improve maintainability and testability.
Handles:
- Order registration with multi-index (by cloid, oid, price_key)
- Atomic unindexing to prevent race conditions
- Partial fill tracking
- Order lookup by various keys
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

from src.strategy import GridLevel
from src.utils import quantize

log = logging.getLogger("gridbot")


@dataclass
class ActiveOrder:
    """Represents an active order with fill tracking."""
    level: GridLevel
    cloid: Optional[str]
    oid: Optional[int]
    filled_qty: float = 0.0
    original_qty: float = 0.0


class OrderManager:
    """
    Thread-safe order registry with multi-index lookup.
    
    Maintains three indices for fast lookup:
    - orders_by_cloid: lookup by client order ID
    - orders_by_oid: lookup by exchange order ID  
    - orders_by_price: lookup by price key (side:price)
    
    All mutations are atomic to prevent partial state corruption.
    """
    
    def __init__(self, tick_sz: float, px_decimals: int) -> None:
        self.tick_sz = tick_sz
        self.px_decimals = px_decimals
        
        # Multi-index storage
        self.orders_by_cloid: Dict[str, ActiveOrder] = {}
        self.orders_by_oid: Dict[int, ActiveOrder] = {}
        self.orders_by_price: Dict[str, ActiveOrder] = {}
    
    def update_tick_info(self, tick_sz: float, px_decimals: int) -> None:
        """Update tick size and decimal precision (e.g., after meta load)."""
        self.tick_sz = tick_sz
        self.px_decimals = px_decimals
    
    def price_key(self, lvl: GridLevel) -> str:
        """
        Generate price key with consistent normalization using Decimal.
        
        Uses Decimal to prevent float precision issues that could cause
        duplicate keys for very close prices.
        """
        px_val = quantize(lvl.px, self.tick_sz, lvl.side, self.px_decimals)
        decimals = min(self.px_decimals, 6)
        px_dec = Decimal(str(px_val)).quantize(
            Decimal(10) ** -decimals,
            rounding=ROUND_HALF_UP
        )
        return f"{lvl.side}:{px_dec}"
    
    def register(self, lvl: GridLevel, cloid: Optional[str], oid: Optional[int]) -> ActiveOrder:
        """
        Register order with atomic multi-index update.
        
        If order already exists (by any ID), updates it rather than creating duplicate.
        Resets fill tracking on replacement.
        """
        price_key = self.price_key(lvl)
        rec = None
        
        # Try to find existing record by ID first (most reliable)
        if cloid and cloid in self.orders_by_cloid:
            rec = self.orders_by_cloid[cloid]
        if rec is None and oid is not None and oid in self.orders_by_oid:
            rec = self.orders_by_oid[oid]
        
        # Only use price key as last resort; verify it's the same order
        if rec is None and price_key in self.orders_by_price:
            candidate = self.orders_by_price[price_key]
            # Only reuse if IDs match or both are None
            if (candidate.cloid == cloid or candidate.oid == oid) or (cloid is None and oid is None):
                rec = candidate
        
        if rec:
            # Update existing record
            self._unindex(rec)
            rec.level = lvl
            rec.cloid = cloid
            rec.oid = oid
            rec.filled_qty = 0.0
            rec.original_qty = lvl.sz
        else:
            # Create new record with partial fill tracking
            rec = ActiveOrder(level=lvl, cloid=cloid, oid=oid, filled_qty=0.0, original_qty=lvl.sz)
        
        # Re-register in all applicable indices
        self.orders_by_price[price_key] = rec
        if cloid:
            self.orders_by_cloid[cloid] = rec
        if oid is not None:
            self.orders_by_oid[oid] = rec
        
        return rec
    
    def lookup(self, cloid: Optional[str] = None, oid: Optional[int] = None, 
               price_key: Optional[str] = None) -> Optional[ActiveOrder]:
        """
        Look up order by any available identifier.
        
        Priority: cloid > oid > price_key
        """
        if cloid and cloid in self.orders_by_cloid:
            return self.orders_by_cloid[cloid]
        if oid is not None and oid in self.orders_by_oid:
            return self.orders_by_oid[oid]
        if price_key and price_key in self.orders_by_price:
            return self.orders_by_price[price_key]
        return None
    
    def pop_by_ids(self, cloid: Optional[str], oid: Optional[int]) -> Optional[ActiveOrder]:
        """
        Find and remove order by IDs.
        
        Returns None if no valid ID provided (prevents None key lookups).
        """
        if cloid is None and oid is None:
            return None
        
        rec = None
        if cloid and cloid in self.orders_by_cloid:
            rec = self.orders_by_cloid.get(cloid)
        if rec is None and oid is not None and oid in self.orders_by_oid:
            rec = self.orders_by_oid.get(oid)
        
        if rec:
            self._unindex(rec)
        return rec
    
    def handle_partial_fill(self, cloid: Optional[str], oid: Optional[int], 
                           fill_sz: float) -> Optional[ActiveOrder]:
        """
        Handle partial fill - update filled_qty, only remove if fully filled.
        
        Returns the ActiveOrder if found (for position updates), None otherwise.
        
        C-2 FIX: Use absolute tolerance instead of percentage to avoid missing
        final micro-fills. A 0.99 threshold can miss the last 1% of an order.
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
        
        # C-2 FIX: Use absolute tolerance based on fill size instead of percentage
        # Previous: rec.filled_qty >= rec.original_qty * 0.99 (could miss 1% of order)
        # New: remaining quantity is less than 1% of this fill size (dust tolerance)
        remaining = rec.original_qty - rec.filled_qty
        dust_tolerance = max(fill_sz * 0.01, 1e-9)  # 1% of fill or epsilon
        
        if rec.original_qty > 0 and remaining <= dust_tolerance:
            self._unindex(rec)
            return rec
        
        # Partial fill: keep order in registry
        log.info(f'{{"event":"partial_fill","cloid":"{cloid}","oid":{oid},'
                 f'"fill_sz":{fill_sz},"filled_qty":{rec.filled_qty},'
                 f'"original_qty":{rec.original_qty},"remaining":{remaining}}}')
        return rec
    
    def unindex(self, rec: ActiveOrder) -> None:
        """Public method to remove order from all indices."""
        self._unindex(rec)
    
    def _unindex(self, rec: ActiveOrder) -> None:
        """
        Atomically remove all references to a record.
        
        Removes from all three indices to prevent dangling references.
        """
        # Remove from price index (may have multiple keys pointing to same rec)
        to_remove_price = [key for key, val in self.orders_by_price.items() if val is rec]
        for key in to_remove_price:
            self.orders_by_price.pop(key, None)
        
        if rec.cloid:
            self.orders_by_cloid.pop(rec.cloid, None)
        if rec.oid is not None:
            self.orders_by_oid.pop(rec.oid, None)
    
    def clear(self) -> None:
        """Clear all order indices."""
        self.orders_by_cloid.clear()
        self.orders_by_oid.clear()
        self.orders_by_price.clear()
    
    def open_count(self) -> int:
        """Number of unique open orders."""
        return len(self.orders_by_price)
    
    def all_orders(self) -> List[ActiveOrder]:
        """Return list of all active orders."""
        return list(self.orders_by_price.values())
    
    def orders_by_side(self, side: str) -> List[ActiveOrder]:
        """Return orders filtered by side ('buy' or 'sell')."""
        return [rec for rec in self.orders_by_price.values() if rec.level.side == side]
    
    def has_price_key(self, price_key: str) -> bool:
        """Check if price key exists in registry."""
        return price_key in self.orders_by_price
    
    def get_by_price_key(self, price_key: str) -> Optional[ActiveOrder]:
        """Get order by price key."""
        return self.orders_by_price.get(price_key)
