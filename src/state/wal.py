"""
Write-Ahead Log (WAL) for crash-safe position tracking.

Provides durability guarantees for fill processing:
1. Fill is written to WAL (fsync) BEFORE position update
2. On crash recovery, WAL is replayed to reconstruct state
3. Periodic checkpoints reduce recovery time

This ensures no fills are lost even if the bot crashes mid-processing.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
import logging

log = logging.getLogger("gridbot")


class WALEntryType(Enum):
    """Types of WAL entries."""
    FILL = "fill"
    POSITION_UPDATE = "position_update"
    ORDER_SUBMIT = "order_submit"
    ORDER_CANCEL = "order_cancel"
    CHECKPOINT = "checkpoint"
    GRID_REBUILD = "grid_rebuild"


@dataclass
class WALEntry:
    """Single WAL entry."""
    sequence: int
    entry_type: WALEntryType
    timestamp_ms: int
    data: Dict[str, Any]
    checksum: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "seq": self.sequence,
            "type": self.entry_type.value,
            "ts": self.timestamp_ms,
            "data": self.data,
            "csum": self.checksum,
        }
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WALEntry":
        return cls(
            sequence=d["seq"],
            entry_type=WALEntryType(d["type"]),
            timestamp_ms=d["ts"],
            data=d["data"],
            checksum=d.get("csum"),
        )


@dataclass
class WALCheckpoint:
    """Checkpoint state for fast recovery."""
    sequence: int
    timestamp_ms: int
    position: float
    realized_pnl: float
    last_fill_time_ms: int
    open_order_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "seq": self.sequence,
            "ts": self.timestamp_ms,
            "position": self.position,
            "realized_pnl": self.realized_pnl,
            "last_fill_ms": self.last_fill_time_ms,
            "open_orders": self.open_order_count,
        }
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WALCheckpoint":
        return cls(
            sequence=d["seq"],
            timestamp_ms=d["ts"],
            position=d["position"],
            realized_pnl=d["realized_pnl"],
            last_fill_time_ms=d["last_fill_ms"],
            open_order_count=d["open_orders"],
        )


class WriteAheadLog:
    """
    Crash-safe Write-Ahead Log for position tracking.
    
    Guarantees:
    - All fills are durably recorded before position updates
    - Recovery replays WAL from last checkpoint
    - Checkpoints reduce replay time
    
    Usage:
        wal = WriteAheadLog(coin, state_dir)
        await wal.initialize()
        
        # Before updating position
        await wal.append_fill(fill_data, position_before)
        
        # Periodic checkpointing
        await wal.checkpoint(position, pnl, last_fill_ms, order_count)
        
        # On recovery
        checkpoint, entries = await wal.recover()
    """
    
    # Checkpoint every N entries
    CHECKPOINT_INTERVAL = 100
    # Max WAL size before forced rotation (5MB)
    MAX_WAL_SIZE = 5 * 1024 * 1024
    # Keep N old WAL files
    MAX_WAL_FILES = 3
    
    def __init__(
        self,
        coin: str,
        state_dir: str,
        on_error: Optional[Callable[[str], None]] = None,
        fsync: bool = True,
    ) -> None:
        """
        Initialize WAL.
        
        Args:
            coin: Trading symbol
            state_dir: Directory for WAL files
            on_error: Callback for write errors
            fsync: Whether to fsync after each write (disable for testing)
        """
        safe_coin = coin.replace(":", "_").replace("/", "_")
        self.coin = coin
        self.state_dir = Path(state_dir)
        self.wal_path = self.state_dir / f"wal_{safe_coin}.log"
        self.checkpoint_path = self.state_dir / f"wal_{safe_coin}.checkpoint"
        self._on_error = on_error
        self._fsync = fsync
        
        self._sequence: int = 0
        self._last_checkpoint_seq: int = 0
        self._lock = asyncio.Lock()
        self._file_handle: Optional[Any] = None
        self._entries_since_checkpoint: int = 0
        
        # Statistics
        self._stats = {
            "entries_written": 0,
            "checkpoints": 0,
            "recoveries": 0,
            "write_errors": 0,
        }
    
    async def initialize(self) -> None:
        """Initialize WAL, creating directory if needed."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
        await self._open_wal()
    
    async def _open_wal(self) -> None:
        """Open WAL file for appending."""
        loop = asyncio.get_running_loop()
        
        def _open():
            return open(self.wal_path, "a", encoding="utf-8")
        
        self._file_handle = await loop.run_in_executor(None, _open)
    
    async def close(self) -> None:
        """Close WAL file."""
        if self._file_handle:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._file_handle.close)
            self._file_handle = None
    
    def _compute_checksum(self, data: Dict[str, Any]) -> str:
        """Compute simple checksum for entry integrity."""
        import hashlib
        content = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return hashlib.md5(content.encode()).hexdigest()[:8]
    
    async def append(
        self,
        entry_type: WALEntryType,
        data: Dict[str, Any],
    ) -> int:
        """
        Append entry to WAL with fsync.
        
        Returns sequence number of the entry.
        """
        async with self._lock:
            self._sequence += 1
            
            entry = WALEntry(
                sequence=self._sequence,
                entry_type=entry_type,
                timestamp_ms=int(time.time() * 1000),
                data=data,
            )
            entry.checksum = self._compute_checksum(entry.to_dict())
            
            line = json.dumps(entry.to_dict(), separators=(",", ":")) + "\n"
            
            loop = asyncio.get_running_loop()
            
            def _write_and_sync():
                try:
                    self._file_handle.write(line)
                    if self._fsync:
                        self._file_handle.flush()
                        os.fsync(self._file_handle.fileno())
                    return None
                except Exception as e:
                    return str(e)
            
            error = await loop.run_in_executor(None, _write_and_sync)
            
            if error:
                self._stats["write_errors"] += 1
                if self._on_error:
                    self._on_error(error)
                raise IOError(f"WAL write failed: {error}")
            
            self._stats["entries_written"] += 1
            self._entries_since_checkpoint += 1
            
            return self._sequence
    
    async def append_fill(
        self,
        fill: Dict[str, Any],
        position_before: float,
        position_after: float,
    ) -> int:
        """
        Append fill entry with position context.
        
        This MUST be called BEFORE updating in-memory position.
        """
        return await self.append(
            WALEntryType.FILL,
            {
                "fill": fill,
                "pos_before": position_before,
                "pos_after": position_after,
            },
        )
    
    async def append_order_submit(
        self,
        cloid: Optional[str],
        side: str,
        price: float,
        size: float,
    ) -> int:
        """Append order submission entry."""
        return await self.append(
            WALEntryType.ORDER_SUBMIT,
            {
                "cloid": cloid,
                "side": side,
                "price": price,
                "size": size,
            },
        )
    
    async def checkpoint(
        self,
        position: float,
        realized_pnl: float,
        last_fill_time_ms: int,
        open_order_count: int,
    ) -> None:
        """
        Write checkpoint for faster recovery.
        
        Checkpoints capture current state so recovery only needs to
        replay entries since the checkpoint.
        """
        async with self._lock:
            checkpoint = WALCheckpoint(
                sequence=self._sequence,
                timestamp_ms=int(time.time() * 1000),
                position=position,
                realized_pnl=realized_pnl,
                last_fill_time_ms=last_fill_time_ms,
                open_order_count=open_order_count,
            )
            
            loop = asyncio.get_running_loop()
            
            def _write_checkpoint():
                # Write to temp file first, then atomic rename
                tmp_path = self.checkpoint_path.with_suffix(".tmp")
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(checkpoint.to_dict(), f)
                    f.flush()
                    os.fsync(f.fileno())
                tmp_path.replace(self.checkpoint_path)
            
            await loop.run_in_executor(None, _write_checkpoint)
            
            self._last_checkpoint_seq = self._sequence
            self._entries_since_checkpoint = 0
            self._stats["checkpoints"] += 1
            
            # Rotate WAL if too large
            await self._maybe_rotate()
    
    async def _maybe_rotate(self) -> None:
        """Rotate WAL if it exceeds size limit."""
        loop = asyncio.get_running_loop()
        
        def _check_size():
            try:
                return self.wal_path.stat().st_size
            except FileNotFoundError:
                return 0
        
        size = await loop.run_in_executor(None, _check_size)
        
        if size > self.MAX_WAL_SIZE:
            await self._rotate()
    
    async def _rotate(self) -> None:
        """Rotate WAL file."""
        if self._file_handle:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._file_handle.close)
        
        loop = asyncio.get_running_loop()
        
        def _do_rotate():
            # Shift existing rotated files
            for i in range(self.MAX_WAL_FILES - 1, 0, -1):
                old_path = self.wal_path.with_suffix(f".{i}")
                new_path = self.wal_path.with_suffix(f".{i + 1}")
                if old_path.exists():
                    if i + 1 > self.MAX_WAL_FILES:
                        old_path.unlink()
                    else:
                        old_path.replace(new_path)
            
            # Rotate current WAL
            if self.wal_path.exists():
                self.wal_path.replace(self.wal_path.with_suffix(".1"))
        
        await loop.run_in_executor(None, _do_rotate)
        await self._open_wal()
    
    async def recover(self) -> tuple[Optional[WALCheckpoint], List[WALEntry]]:
        """
        Recover state from WAL.
        
        Returns:
            Tuple of (checkpoint or None, entries since checkpoint)
        """
        async with self._lock:
            loop = asyncio.get_running_loop()
            
            def _load_checkpoint() -> Optional[WALCheckpoint]:
                if not self.checkpoint_path.exists():
                    return None
                try:
                    with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    return WALCheckpoint.from_dict(data)
                except Exception as e:
                    log.warning(f"Failed to load checkpoint: {e}")
                    return None
            
            checkpoint = await loop.run_in_executor(None, _load_checkpoint)
            
            def _load_entries() -> List[WALEntry]:
                entries = []
                if not self.wal_path.exists():
                    return entries
                
                checkpoint_seq = checkpoint.sequence if checkpoint else 0
                
                try:
                    with open(self.wal_path, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                data = json.loads(line)
                                entry = WALEntry.from_dict(data)
                                
                                # Only return entries after checkpoint
                                if entry.sequence > checkpoint_seq:
                                    # Verify checksum
                                    expected = entry.checksum
                                    entry.checksum = None
                                    computed = self._compute_checksum(entry.to_dict())
                                    entry.checksum = expected
                                    
                                    if expected and computed != expected:
                                        log.warning(f"WAL entry {entry.sequence} checksum mismatch")
                                        continue
                                    
                                    entries.append(entry)
                            except Exception as e:
                                log.warning(f"Failed to parse WAL entry: {e}")
                                continue
                except Exception as e:
                    log.error(f"Failed to read WAL: {e}")
                
                return entries
            
            entries = await loop.run_in_executor(None, _load_entries)
            
            # Update sequence number
            if entries:
                self._sequence = max(e.sequence for e in entries)
            elif checkpoint:
                self._sequence = checkpoint.sequence
            
            self._stats["recoveries"] += 1
            
            return checkpoint, entries
    
    async def should_checkpoint(self) -> bool:
        """Check if a checkpoint is recommended."""
        return self._entries_since_checkpoint >= self.CHECKPOINT_INTERVAL
    
    def get_stats(self) -> Dict[str, Any]:
        """Get WAL statistics."""
        return {
            **self._stats,
            "sequence": self._sequence,
            "last_checkpoint_seq": self._last_checkpoint_seq,
            "entries_since_checkpoint": self._entries_since_checkpoint,
        }
