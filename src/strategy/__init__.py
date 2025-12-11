"""
Strategy package - Signal & grid logic.

This package contains the grid strategy implementation with ATR/EWMA spacing,
trend bias, and inventory skew.
"""

from src.strategy.strategy import (
    GridLevel,
    GridStrategy,
    VolatilityModel,
    TrendModel,
)
from src.strategy.strategy_factory import StrategyFactory
from src.strategy.grid_calculator import (
    GridCalculator,
    GridBuildResult,
    GridDiff,
)
from src.strategy.grid_snapshot import (
    SnapshotManager,
    GridSnapshot,
    OrderSnapshot,
    GridDiffResult,
    compute_grid_diff_from_snapshot,
)

__all__ = [
    "GridLevel",
    "GridStrategy",
    "VolatilityModel",
    "TrendModel",
    "StrategyFactory",
    "GridCalculator",
    "GridBuildResult",
    "GridDiff",
    "SnapshotManager",
    "GridSnapshot",
    "OrderSnapshot",
    "GridDiffResult",
    "compute_grid_diff_from_snapshot",
]
