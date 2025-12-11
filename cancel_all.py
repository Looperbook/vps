"""Script to cancel all orders and show position for xyz coins."""
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants
import os
import time
import httpx
from dotenv import load_dotenv

# Load environment - try local first, then VPS path
if os.path.exists('.env'):
    load_dotenv('.env')
else:
    load_dotenv('/opt/gridbot/.env')

account_address = os.environ['HL_USER_ADDRESS']
private_key = os.environ['HL_AGENT_KEY']
dex = os.environ.get('HL_DEX', 'xyz')

# Initialize Info with xyz DEX support
info = Info(constants.MAINNET_API_URL, skip_ws=True, perp_dexs=[dex])

# Get positions with DEX parameter
print("=== Current Exchange Positions (xyz DEX) ===")
# Need to use raw API call with dex parameter
client = httpx.Client()
resp = client.post(
    f"{constants.MAINNET_API_URL}/info",
    json={"type": "clearinghouseState", "user": account_address, "dex": dex}
)
state = resp.json()

for p in state.get('assetPositions', []):
    pos = p.get('position', {})
    coin = pos.get('coin', 'unknown')
    size = float(pos.get('szi', 0))
    entry = pos.get('entryPx', '0')
    if size != 0:
        print(f'{coin}: size={size}, entry={entry}')

print(f"\nEquity: {state.get('crossMarginSummary', {}).get('accountValue', 'N/A')}")

# Get open orders with DEX parameter
print("\n=== Open Orders (xyz DEX) ===")
resp = client.post(
    f"{constants.MAINNET_API_URL}/info",
    json={"type": "frontendOpenOrders", "user": account_address, "dex": dex}
)
open_orders = resp.json()
print(f'Total open orders: {len(open_orders)}')

# Count by coin
coins = {}
for o in open_orders:
    coin = o.get('coin', 'unknown')
    coins[coin] = coins.get(coin, 0) + 1

for coin, count in sorted(coins.items()):
    print(f'  {coin}: {count} orders')

# Ask for confirmation
if len(open_orders) > 0:
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--yes':
        confirm = 'yes'
    else:
        confirm = input("\nCancel ALL orders? Type 'yes' to confirm: ")
    
    if confirm.lower() == 'yes':
        print("\nCancelling all orders...")
        from hyperliquid.utils.signing import CancelRequest
        
        # Initialize exchange with perp_dexs for xyz DEX support
        import eth_account
        wallet = eth_account.Account.from_key(private_key)
        
        # For xyz DEX, we need to initialize with perp_dexs parameter
        exchange = Exchange(wallet, constants.MAINNET_API_URL, account_address=account_address, perp_dexs=[dex])
        
        # Build cancel requests - group by coin
        by_coin = {}
        for o in open_orders:
            oid = o.get('oid')
            coin = o.get('coin')
            if oid and coin:
                if coin not in by_coin:
                    by_coin[coin] = []
                by_coin[coin].append(int(oid))
        
        # Cancel each coin's orders
        for coin, oids in by_coin.items():
            print(f"\nCancelling {len(oids)} orders for {coin}...")
            # Cancel in batches
            batch_size = 20
            for i in range(0, len(oids), batch_size):
                batch_oids = oids[i:i+batch_size]
                batch = [CancelRequest(coin=coin, oid=oid) for oid in batch_oids]
                try:
                    result = exchange.bulk_cancel(batch)
                    print(f"  Batch {i//batch_size + 1}: Cancelled {len(batch)} orders - status: {result.get('status', 'ok')}")
                except Exception as e:
                    # Try individual cancels as fallback
                    print(f"  Batch {i//batch_size + 1}: Bulk failed ({e}), trying individual...")
                    for oid in batch_oids:
                        try:
                            result = exchange.cancel(coin, oid)
                            print(f"    Cancelled oid={oid}")
                        except Exception as e2:
                            print(f"    Failed oid={oid}: {e2}")
                time.sleep(0.3)  # Rate limit
        
        print("\n=== Done ===")
    else:
        print("Cancelled. No orders were modified.")