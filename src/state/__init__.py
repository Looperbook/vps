"""
State management package.

This package contains state persistence and management components.
"""

# Import StateStore first (no dependencies)
from src.state.state import StateStore

# These imports have complex dependencies, use lazy imports via __getattr__
# to avoid circular imports
from src.state.state_atomic import AtomicStateStore
from src.state.wal import WriteAheadLog, WALEntryType
from src.state.position_tracker import PositionTracker
from src.state.shadow_ledger import ShadowLedger

def __getattr__(name: str):
    """Lazy import to avoid circular dependencies with execution package."""
    if name == "StateManager":
        from src.state.state_manager import StateManager
        return StateManager
    if name == "StateManagerConfig":
        from src.state.state_manager import StateManagerConfig
        return StateManagerConfig
    if name == "StateSnapshot":
        from src.state.state_manager import StateSnapshot
        return StateSnapshot
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "StateStore",
    "StateManager",
    "StateManagerConfig", 
    "StateSnapshot",
    "PositionTracker",
    "ShadowLedger",
    "AtomicStateStore",
    "WriteAheadLog",
    "WALEntryType",
]
