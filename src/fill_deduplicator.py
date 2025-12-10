"""
FillDeduplicator: Centralized fill deduplication with bounded memory.

Extracted from bot.py to improve maintainability and testability.
Handles:
- Global fill deduplication across WS and REST
- Bounded eviction to prevent memory leaks
- Fill key generation with consistent hashing
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger("gridbot")


class FillDeduplicator:
    """
    Fill deduplication with bounded memory usage.
    
    Maintains a set of processed fill keys with LRU-style eviction
    when the maximum size is reached. This prevents double-counting
    fills across WebSocket and REST poll sources.
    
    M-1 FIX: Also tracks fill sequence to detect gaps when multiple
    orders fill simultaneously during fast wicks.
    
    Thread-safe for single-threaded asyncio usage (no internal locks).
    For multi-threaded access, wrap calls with external synchronization.
    """
    
    def __init__(self, max_fills: int = 10000,
                 log_event: Optional[callable] = None) -> None:
        """
        Initialize deduplicator.
        
        Args:
            max_fills: Maximum fills to track before eviction
            log_event: Optional callback for logging events
        """
        self.max_fills = max_fills
        self._processed: Dict[str, bool] = {}
        self._order: List[str] = []  # For FIFO eviction
        self._log_event = log_event or self._default_log
        self._stats = {
            "processed": 0,
            "duplicates": 0,
            "evictions": 0,
            "sequence_gaps": 0,
        }
        # M-1 FIX: Track last fill timestamp per coin for sequence gap detection
        self._last_fill_ts: Dict[str, int] = {}  # coin -> last fill timestamp ms
    
    def _default_log(self, event: str, **kwargs) -> None:
        """Default logging implementation."""
        log.debug(f'{{"event":"{event}",{",".join(f"{k}:{v}" for k,v in kwargs.items())}}}')
    
    @staticmethod
    def make_fill_key(fill: Dict[str, Any]) -> str:
        """
        Generate unique fill key for deduplication.
        
        Uses combination of oid, cloid, time, px, sz to uniquely
        identify a fill even across different sources.
        """
        return f"{fill.get('oid')}_{fill.get('cloid')}_{fill.get('time')}_{fill.get('px')}_{fill.get('sz')}"
    
    def is_new(self, fill_key: str) -> bool:
        """
        Check if fill is new (not seen before).
        
        Args:
            fill_key: Unique fill identifier from make_fill_key
            
        Returns:
            True if new fill, False if duplicate
        """
        if fill_key in self._processed:
            self._stats["duplicates"] += 1
            return False
        return True
    
    def check_and_add(self, fill: Dict[str, Any]) -> bool:
        """
        Check if fill is new and add to processed set if so.
        
        Convenience method combining make_fill_key, is_new, and mark_processed.
        
        Args:
            fill: Fill dictionary with oid, cloid, time, px, sz
            
        Returns:
            True if fill is new and was added, False if duplicate
        """
        fill_key = self.make_fill_key(fill)
        
        if fill_key in self._processed:
            self._stats["duplicates"] += 1
            self._log_event("fill_dedup_skip", key=fill_key)
            return False
        
        self._add_key(fill_key)
        return True
    
    def mark_processed(self, fill_key: str) -> None:
        """
        Mark a fill key as processed.
        
        Args:
            fill_key: Unique fill identifier
        """
        if fill_key not in self._processed:
            self._add_key(fill_key)
    
    def _add_key(self, fill_key: str) -> None:
        """Add key with bounded eviction."""
        # Evict oldest if at capacity
        if len(self._order) >= self.max_fills:
            old_key = self._order.pop(0)
            self._processed.pop(old_key, None)
            self._stats["evictions"] += 1
        
        self._processed[fill_key] = True
        self._order.append(fill_key)
        self._stats["processed"] += 1
    
    def check_and_add_sequenced(self, fill: Dict[str, Any], coin: str) -> tuple:
        """
        M-1 FIX: Check fill with sequence tracking for gap detection.
        
        Use this when you need to detect if fills arrived out-of-order,
        which can happen during fast wicks when multiple orders fill.
        
        Args:
            fill: Fill dictionary with oid, cloid, time, px, sz
            coin: Coin symbol for per-coin sequence tracking
            
        Returns:
            Tuple of (is_new: bool, is_gap: bool)
            - is_new: True if fill is new and was added
            - is_gap: True if fill arrived out of sequence (>1s gap)
        """
        fill_key = self.make_fill_key(fill)
        ts = int(fill.get("time", 0))
        
        if fill_key in self._processed:
            self._stats["duplicates"] += 1
            return False, False
        
        # Check for sequence gap (fill arriving out of order by >1 second)
        last_ts = self._last_fill_ts.get(coin, 0)
        is_gap = ts > 0 and last_ts > 0 and ts < last_ts - 1000
        
        if is_gap:
            self._stats["sequence_gaps"] += 1
            self._log_event("fill_sequence_gap", coin=coin, fill_ts=ts, 
                           last_ts=last_ts, gap_ms=last_ts - ts)
        
        self._add_key(fill_key)
        # Update sequence tracker (always use max to handle out-of-order)
        self._last_fill_ts[coin] = max(self._last_fill_ts.get(coin, 0), ts)
        
        return True, is_gap
    
    def get_last_fill_ts(self, coin: str) -> int:
        """Get last fill timestamp for a coin."""
        return self._last_fill_ts.get(coin, 0)
    
    def contains(self, fill_key: str) -> bool:
        """Check if fill key exists in processed set."""
        return fill_key in self._processed
    
    def clear(self) -> None:
        """Clear all tracked fills."""
        self._processed.clear()
        self._order.clear()
    
    def size(self) -> int:
        """Number of fills currently tracked."""
        return len(self._processed)
    
    def get_stats(self) -> dict:
        """Get deduplication statistics."""
        return {
            **self._stats,
            "current_size": self.size(),
            "max_size": self.max_fills,
        }
    
    def resize(self, new_max: int) -> None:
        """
        Resize the deduplicator capacity.
        
        If new_max is smaller than current size, evicts oldest entries.
        """
        self.max_fills = new_max
        while len(self._order) > new_max:
            old_key = self._order.pop(0)
            self._processed.pop(old_key, None)
            self._stats["evictions"] += 1
