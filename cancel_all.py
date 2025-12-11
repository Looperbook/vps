"""Script to cancel all orders and show position for xyz coins."""
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants
import os
import time
import httpx
from dotenv import load_dotenv

# Load environment
load_dotenv('/opt/gridbot/.env')

account_address = os.environ['HL_USER_ADDRESS']
private_key = os.environ['HL_AGENT_KEY']
dex = os.environ.get('HL_DEX', 'xyz')

info = Info(constants.MAINNET_API_URL, skip_ws=True)

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
        
        # Initialize exchange
        exchange = Exchange(None, constants.MAINNET_API_URL, account_address=account_address)
        exchange.account_address = account_address
        # Use agent key for signing
        exchange.wallet = None
        import eth_account
        exchange.wallet = eth_account.Account.from_key(private_key)
        
        # Build cancel requests
        cancel_requests = []
        for o in open_orders:
            oid = o.get('oid')
            coin = o.get('coin')
            if oid and coin:
                cancel_requests.append(CancelRequest(coin=coin, oid=int(oid)))
        
        # Cancel in batches
        batch_size = 20
        for i in range(0, len(cancel_requests), batch_size):
            batch = cancel_requests[i:i+batch_size]
            try:
                result = exchange.bulk_cancel(batch)
                print(f"Batch {i//batch_size + 1}: Cancelled {len(batch)} orders - {result}")
            except Exception as e:
                print(f"Batch {i//batch_size + 1}: Error - {e}")
            time.sleep(0.5)  # Rate limit
        
        print("\n=== Done ===")
    else:
        print("Cancelled. No orders were modified.")