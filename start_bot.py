#!/usr/bin/env python3
"""
Safe Grid Bot Startup Script

This script helps you safely start the bot by:
1. Checking that .env file exists
2. Validating configuration
3. Running pre-flight checks
4. Starting bot with proper error handling
"""

import os
import sys
import json
from pathlib import Path

def check_env_file():
    """Verify .env file exists and has required keys."""
    env_file = Path('.env')
    
    if not env_file.exists():
        print("❌ ERROR: .env file not found")
        print("\nCreate .env file with at least one of:")
        print("  HL_PRIVATE_KEY=0x...  (full wallet control)")
        print("  OR")
        print("  HL_AGENT_KEY=0x...    (agent key)")
        print("  HL_USER_ADDRESS=0x... (wallet address for agent)")
        return False
    
    with open(env_file) as f:
        content = f.read()
        has_private = 'HL_PRIVATE_KEY' in content
        has_agent = 'HL_AGENT_KEY' in content and 'HL_USER_ADDRESS' in content
        if not has_private and not has_agent:
            print("❌ ERROR: Missing credentials in .env")
            print("  Need either HL_PRIVATE_KEY or (HL_AGENT_KEY + HL_USER_ADDRESS)")
            return False
    
    print("✅ .env file present and valid")
    return True

def check_config():
    """Validate per_coin.yaml configuration."""
    config_file = Path('configs/per_coin.yaml')
    
    if not config_file.exists():
        print("❌ ERROR: configs/per_coin.yaml not found")
        return False
    
    with open(config_file) as f:
        content = f.read()
        if 'xyz:' not in content:
            print("❌ ERROR: No coins configured in per_coin.yaml")
            return False
    
    print("✅ Configuration file present and valid")
    return True

def check_state_directory():
    """Ensure state directory exists."""
    state_dir = Path('state')
    if not state_dir.exists():
        state_dir.mkdir(parents=True)
    
    print("✅ State directory ready")
    return True

def check_logs_directory():
    """Ensure logs directory exists."""
    logs_dir = Path('logs')
    if not logs_dir.exists():
        logs_dir.mkdir(parents=True)
    
    print("✅ Logs directory ready")
    return True

def get_environment_mode():
    """Determine if running on mainnet or testnet based on base URL."""
    from dotenv import load_dotenv
    load_dotenv()
    base_url = os.getenv('HL_BASE_URL', 'https://api.hyperliquid.xyz')
    testnet = 'testnet' in base_url.lower()
    mode = 'TESTNET' if testnet else 'MAINNET'
    color = '🟡' if testnet else '🔴'
    
    print(f"\n{color} Running on: {mode}")
    print(f"   API: {base_url}")
    
    if testnet:
        print("   ✅ Testnet (safe for learning)")
    else:
        print("   ⚠️  MAINNET (real money!)")
        print("   ⚠️  Make sure you've tested on testnet first!")
    
    return testnet

def confirm_startup(auto_confirm: bool = False):
    """Get user confirmation before starting."""
    print("\n" + "="*60)
    print("PRE-FLIGHT CHECKS")
    print("="*60)
    
    checks = [
        ("Environment file", check_env_file),
        ("Configuration file", check_config),
        ("State directory", check_state_directory),
        ("Logs directory", check_logs_directory),
    ]
    
    all_passed = True
    for name, check_func in checks:
        if not check_func():
            all_passed = False
    
    if not all_passed:
        print("\n❌ Pre-flight checks FAILED")
        print("Fix errors above and try again")
        return False
    
    print("\n✅ All pre-flight checks passed!")
    
    # Determine environment
    testnet = get_environment_mode()
    
    # Skip confirmation if auto_confirm (for systemd)
    if auto_confirm:
        print("\n✅ Auto-confirm enabled (--no-confirm)")
        print("✅ Starting bot...")
        return True
    
    # Get user confirmation
    print("\n" + "="*60)
    print("STARTUP CONFIRMATION")
    print("="*60)
    
    print("\nBefore starting, confirm:")
    print("  □ You've read DEPLOYMENT_GUIDE.md")
    print("  □ Configuration amounts are correct")
    print("  □ Risk limits are appropriate")
    if not testnet:
        print("  □ You understand real money is at risk")
        print("  □ You've tested on testnet first")
    
    response = input("\nType 'START' to continue: ").strip().upper()
    
    if response != 'START':
        print("❌ Startup cancelled")
        return False
    
    print("\n✅ Starting bot...")
    return True

def main():
    """Run pre-flight checks and start bot."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Grid Trading Bot')
    parser.add_argument('--no-confirm', action='store_true',
                        help='Skip startup confirmation (for systemd/automated use)')
    args = parser.parse_args()
    
    try:
        if not confirm_startup(auto_confirm=args.no_confirm):
            sys.exit(1)
        
        # Import and run bot
        import asyncio
        from src.main import main as bot_main
        asyncio.run(bot_main())
        
        print("\n\n✅ Bot stopped gracefully")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Bot shutdown requested (Ctrl+C)")
        print("Cleaning up...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nCheck logs/gridbot.json for details")
        sys.exit(1)

if __name__ == '__main__':
    main()
    print("\n💡 Bot session ended. Terminal ready for next command.")
