"""
Pytest configuration and fixtures.
Adds src/ to Python path so tests can import from src.
"""

import sys
from pathlib import Path

# Add the repo root's src/ directory to sys.path
repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
