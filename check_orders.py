"""Check open orders on xyz DEX."""
import httpx
from dotenv import load_dotenv
import os

load_dotenv()

client = httpx.Client()
user = os.environ['HL_USER_ADDRESS']

# Get open orders
resp = client.post(
    'https://api.hyperliquid.xyz/info',
    json={'type': 'frontendOpenOrders', 'user': user, 'dex': 'xyz'}
)
orders = resp.json()

print(f"Open orders: {len(orders)}")
coins = {}
for o in orders:
    c = o.get('coin', '?')
    coins[c] = coins.get(c, 0) + 1
for c, n in sorted(coins.items()):
    print(f"  {c}: {n}")

# Get positions
resp = client.post(
    'https://api.hyperliquid.xyz/info',
    json={'type': 'clearinghouseState', 'user': user, 'dex': 'xyz'}
)
state = resp.json()

print("\nPositions:")
for p in state.get('assetPositions', []):
    pos = p.get('position', {})
    coin = pos.get('coin', 'unknown')
    size = float(pos.get('szi', 0))
    if size != 0:
        print(f"  {coin}: {size}")

print(f"\nEquity: {state.get('crossMarginSummary', {}).get('accountValue', 'N/A')}")
