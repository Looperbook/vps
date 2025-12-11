"""
Event-sourced fill log.

Append-only newline-delimited JSON file per coin storing fills as they arrive.
Provides safe async append and reading of events since a timestamp to enable
replay-based reconciliation on startup.

Includes BatchedFillLog for optimized batch writes (Optimization-2).
"""

from __future__ import annotations

import asyncio
import gzip
import shutil
import os
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from src.core.json_utils import dumps as json_dumps, loads as json_loads


class FillLog:
    def __init__(self, coin: str, state_dir: str, max_bytes: int = 5 * 1024 * 1024,
                 on_write_error: Optional[Callable[[str], None]] = None) -> None:
        """Event-sourced newline-delimited JSON fill log.

        - `max_bytes`: approximate threshold to trigger rotation.
        - `on_write_error`: callback when write fails (for metrics/alerts)
        """
        safe = coin.replace(":", "_").replace("/", "_")
        self.path = Path(state_dir) / f"fill_log_{safe}.log"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._max_bytes = int(max_bytes)
        self._on_write_error = on_write_error
        self._write_errors = 0

    async def append(self, event: Dict[str, Any]) -> None:
        """Append an event to the log using file append mode to avoid reading whole file.

        Performs minimal checks and periodically rotates the file when size exceeds threshold.
        Calls on_write_error callback if write fails.
        """
        line = json_dumps(event) + "\n"
        write_error_msg = None
        async with self._lock:
            loop = asyncio.get_running_loop()

            def _append_and_maybe_rotate() -> Optional[str]:
                nonlocal write_error_msg
                try:
                    # ensure parent exists
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    # append in binary-safe mode
                    with self.path.open("a", encoding="utf-8") as fh:
                        fh.write(line)
                    try:
                        if self.path.stat().st_size > self._max_bytes:
                            # rotate: rename to .old with incremental suffix
                            idx = 1
                            while True:
                                dest = self.path.with_suffix(f".old{idx}")
                                if not dest.exists():
                                    # move current log to rotated name
                                    self.path.replace(dest)
                                    # compress rotated file to .gz to save disk
                                    try:
                                        gz_path = Path(str(dest) + ".gz")
                                        with dest.open("rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                                            shutil.copyfileobj(f_in, f_out)
                                        try:
                                            dest.unlink()
                                        except Exception:
                                            pass
                                        # optional archival upload if configured via env
                                        try:
                                            bucket = os.getenv("HL_ARCHIVE_S3_BUCKET")
                                            if bucket:
                                                try:
                                                    import boto3
                                                    s3 = boto3.client("s3")
                                                    s3.upload_file(str(gz_path), bucket, gz_path.name)
                                                except Exception:
                                                    pass
                                        except Exception:
                                            pass
                                    except Exception:
                                        # if compression/upload fails, leave rotated file in place
                                        pass
                                    break
                                idx += 1
                    except Exception:
                        pass
                    return None
                except Exception as e:
                    # Track error for callback
                    return str(e)

            write_error_msg = await loop.run_in_executor(None, _append_and_maybe_rotate)
        
        # Call error callback outside the lock
        if write_error_msg:
            self._write_errors += 1
            if self._on_write_error:
                try:
                    self._on_write_error(write_error_msg)
                except Exception:
                    pass

            await loop.run_in_executor(None, _append_and_maybe_rotate)

    async def read_since(self, start_ms: int) -> List[Dict[str, Any]]:
        """Read events with `time` > `start_ms`.

        Reads the full current log in executor under the lock.
        """
        async with self._lock:
            loop = asyncio.get_running_loop()

            def _read_lines() -> List[Dict[str, Any]]:
                if not self.path.exists():
                    return []
                out: List[Dict[str, Any]] = []
                try:
                    with self.path.open("r", encoding="utf-8") as fh:
                        for ln in fh:
                            ln = ln.strip()
                            if not ln:
                                continue
                            try:
                                obj = json_loads(ln)
                                t = int(obj.get("time", 0))
                                if t > start_ms:
                                    out.append(obj)
                            except Exception:
                                continue
                except Exception:
                    return []
                return out

            return await loop.run_in_executor(None, _read_lines)

    async def compact(self, keep_since_ms: int) -> None:
        """Truncate the log keeping only events newer than `keep_since_ms`.

        This runs in the executor and acquires the lock to prevent concurrent appends.
        """
        async with self._lock:
            loop = asyncio.get_running_loop()

            def _compact():
                if not self.path.exists():
                    return
                lines = []
                try:
                    with self.path.open("r", encoding="utf-8") as fh:
                        for ln in fh:
                            ln = ln.strip()
                            if not ln:
                                continue
                            try:
                                obj = json_loads(ln)
                                t = int(obj.get("time", 0))
                                if t > keep_since_ms:
                                    lines.append(ln + "\n")
                            except Exception:
                                continue
                    with self.path.open("w", encoding="utf-8") as fh:
                        fh.writelines(lines)
                except Exception:
                    return

            await loop.run_in_executor(None, _compact)


class BatchedFillLog(FillLog):
    """
    Optimization-2: Batched fill log with periodic flush.
    
    Instead of writing each fill immediately to disk, this buffers writes in memory
    and flushes them periodically (default: every 1 second) or when buffer reaches
    a threshold. This reduces I/O overhead significantly during high-fill-rate periods.
    
    The buffer is flushed:
    - Periodically via background task (flush_interval)
    - When buffer exceeds max_buffer_size
    - On explicit flush() call
    - On stop() for graceful shutdown
    
    Thread-safety: Uses asyncio.Lock for buffer access; not designed for multi-threaded use.
    """

    def __init__(
        self,
        coin: str,
        state_dir: str,
        max_bytes: int = 5 * 1024 * 1024,
        flush_interval: float = 1.0,
        max_buffer_size: int = 100,
        on_write_error: Optional[Callable[[str], None]] = None,
    ) -> None:
        super().__init__(coin, state_dir, max_bytes, on_write_error=on_write_error)
        self._buffer: List[str] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_interval = flush_interval
        self._max_buffer_size = max_buffer_size
        self._flush_task: Optional[asyncio.Task] = None
        self._stopping = False
        self._logger = logging.getLogger("gridbot")

    async def start(self) -> None:
        """Start the background flush loop."""
        if self._flush_task is None:
            self._stopping = False
            self._flush_task = asyncio.create_task(self._flush_loop())
            self._logger.info(json_dumps({
                "event": "batched_fill_log_start",
                "path": str(self.path),
                "flush_interval": self._flush_interval,
            }))

    async def stop(self) -> None:
        """Stop the flush loop and flush remaining buffer."""
        self._stopping = True
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        # Final flush on shutdown
        await self._flush()
        self._logger.info(json_dumps({
            "event": "batched_fill_log_stop",
            "path": str(self.path),
        }))

    async def append(self, event: Dict[str, Any]) -> None:
        """Buffer an event for later batch write.
        
        If buffer exceeds max_buffer_size, triggers immediate flush.
        """
        line = json_dumps(event) + "\n"
        should_flush = False
        async with self._buffer_lock:
            self._buffer.append(line)
            if len(self._buffer) >= self._max_buffer_size:
                should_flush = True
        if should_flush:
            await self._flush()

    async def _flush_loop(self) -> None:
        """Background loop that periodically flushes the buffer."""
        while not self._stopping:
            try:
                await asyncio.sleep(self._flush_interval)
                await self._flush()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._logger.warning(json_dumps({
                    "event": "batched_fill_log_flush_error",
                    "err": str(exc),
                }))

    async def _flush(self) -> None:
        """Write all buffered lines to disk in a single operation."""
        async with self._buffer_lock:
            if not self._buffer:
                return
            lines = self._buffer
            self._buffer = []
        
        # Write all lines in single operation under the file lock
        await self._write_lines_to_disk(lines)
    
    async def flush_sync(self) -> None:
        """
        M-5 FIX: Public method for synchronous flush (WAL semantics).
        
        Call this after critical state updates to ensure durability.
        Forces all buffered writes to disk immediately.
        """
        await self._flush()
    
    async def _write_lines_to_disk(self, lines: List[str]) -> None:
        """Write lines to disk with rotation check."""
        async with self._lock:
            loop = asyncio.get_running_loop()

            def _write_batch():
                try:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    with self.path.open("a", encoding="utf-8") as fh:
                        fh.writelines(lines)
                    # Check rotation after batch write
                    try:
                        if self.path.stat().st_size > self._max_bytes:
                            self._rotate_sync()
                    except Exception:
                        pass
                    return None
                except Exception as exc:
                    # Log but don't raise - append is best-effort
                    self._logger.warning(json_dumps({
                        "event": "batched_fill_log_write_error",
                        "err": str(exc),
                        "lines_lost": len(lines),
                    }))
                    return str(exc)

            write_error = await loop.run_in_executor(None, _write_batch)
        
        # Call error callback outside the lock
        if write_error:
            self._write_errors += 1
            if self._on_write_error:
                try:
                    self._on_write_error(write_error)
                except Exception:
                    pass

    def _rotate_sync(self) -> None:
        """Synchronous rotation logic (called from executor)."""
        idx = 1
        while True:
            dest = self.path.with_suffix(f".old{idx}")
            if not dest.exists():
                self.path.replace(dest)
                try:
                    gz_path = Path(str(dest) + ".gz")
                    with dest.open("rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                    try:
                        dest.unlink()
                    except Exception:
                        pass
                    # Optional S3 archival
                    try:
                        bucket = os.getenv("HL_ARCHIVE_S3_BUCKET")
                        if bucket:
                            import boto3
                            s3 = boto3.client("s3")
                            s3.upload_file(str(gz_path), bucket, gz_path.name)
                    except Exception:
                        pass
                except Exception:
                    pass
                break
            idx += 1

    async def read_since(self, start_ms: int) -> List[Dict[str, Any]]:
        """Read events, including any buffered but not yet flushed.
        
        Flushes buffer first to ensure consistency.
        """
        # Flush buffer first to ensure we read complete data
        await self._flush()
        return await super().read_since(start_ms)

    async def compact(self, keep_since_ms: int) -> None:
        """Compact the log, flushing buffer first."""
        await self._flush()
        await super().compact(keep_since_ms)

    def buffer_size(self) -> int:
        """Return current buffer size (for monitoring)."""
        # Note: Not async-safe, but useful for metrics
        return len(self._buffer)
