from src.per_coin_config import load_per_coin_overrides
from pathlib import Path
import os
import sys
from types import ModuleType


def test_load_per_coin_overrides(tmp_path, monkeypatch):
    p = tmp_path / "pc.yaml"
    p.write_text("BTC: { coalesce_ms: 25 }\nXYZ: { default_strategy: grid }\n")
    monkeypatch.setenv("HL_PER_COIN_CONFIG", str(p))
    # provide a fake yaml module if pyyaml not installed
    import src.per_coin_config as mod
    if "yaml" not in sys.modules:
        fake_yaml = ModuleType("yaml")

        def safe_load(s):
            # accept file-like or string
            if hasattr(s, "read"):
                s = s.read()
            # very small parser for the test format
            out = {}
            for ln in s.splitlines():
                if not ln.strip():
                    continue
                k, rest = ln.split(":", 1)
                k = k.strip()
                body = rest.strip()
                # parse simple mapping { key: value }
                inner = {}
                if body.startswith("{") and body.endswith("}"):
                    body = body[1:-1]
                    for part in body.split(","):
                        if ":" in part:
                            a, b = part.split(":", 1)
                            inner[a.strip()] = int(b.strip()) if b.strip().isdigit() else b.strip()
                out[k] = inner
            return out

        fake_yaml.safe_load = safe_load
        monkeypatch.setitem(sys.modules, "yaml", fake_yaml)
        # ensure the imported module uses our fake yaml reference
        mod.yaml = fake_yaml

    overrides = load_per_coin_overrides()
    assert isinstance(overrides, dict)
    assert "BTC" in overrides
    assert overrides["BTC"]["coalesce_ms"] == 25
