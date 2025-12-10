# Hyperliquid Grid Trading Bot

Production-ready grid trading bot for Hyperliquid perpetual futures. Features async architecture, multi-coin support, atomic state persistence, risk management, and comprehensive observability.

## Quick Start

1. **Setup environment**:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. **Configure** (.env):
   ```env
   HL_BASE_URL=https://api.hyperliquid.xyz
   HL_DEX=xyz
   HL_COINS=xyz:NVDA,xyz:ORCL
   HL_AGENT_KEY=your_agent_key
   HL_USER_ADDRESS=your_wallet_address
   HL_INVESTMENT_USD=1500
   HL_NUM_GRIDS=30
   ```

3. **Run**: `python -m src.main`

## Project Structure

```
grid/
 src/               # Bot source code
 tests/             # Test suite (32 tests)
 configs/           # Configuration files
 .env               # Environment config
 start_bot.py       # Safe startup script
```

## Key Features

- Multi-coin parallel trading
- Atomic state persistence
- Risk management (position limits, drawdown protection)
- Order batching and coalescing
- WebSocket price feeds
- Prometheus metrics
- Per-coin configuration overrides

## Testing

`pytest tests/ -v`  (32 tests, all passing)

## Architecture

- **Entry**: src/main.py
- **Core**: src/bot.py (1300+ lines)
- **Config**: .env + configs/per_coin.yaml
- **State**: Atomic file-based persistence
- **Observability**: JSON logs + Prometheus
