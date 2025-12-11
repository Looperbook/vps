#!/bin/bash
#
# VPS Complete Cleanup Script
# WARNING: This will DELETE all bot data, logs, and state!
#
# Usage: ./clean_vps.sh
#

set -e

echo "🧹 VPS Complete Cleanup Starting..."
echo "⚠️  WARNING: This will delete ALL bot data!"
read -p "Are you sure? (type 'yes' to confirm): " -r
if [[ ! $REPLY =~ ^yes$ ]]; then
    echo "Aborted."
    exit 1
fi

# Stop the bot service if running
echo "🛑 Stopping gridbot service..."
sudo systemctl stop gridbot || true
sudo systemctl disable gridbot || true

# Remove service file
echo "🗑️  Removing systemd service..."
sudo rm -f /etc/systemd/system/gridbot.service
sudo systemctl daemon-reload

# Navigate to bot directory (adjust if different)
BOT_DIR="/root/grid"
if [ ! -d "$BOT_DIR" ]; then
    BOT_DIR="$HOME/grid"
fi

if [ -d "$BOT_DIR" ]; then
    cd "$BOT_DIR"
    
    # Remove virtual environment
    echo "🗑️  Removing virtual environment..."
    rm -rf .venv venv env
    
    # Remove Python cache
    echo "🗑️  Removing Python cache..."
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete 2>/dev/null || true
    rm -rf .pytest_cache
    
    # Remove state and logs
    echo "🗑️  Removing state and logs..."
    rm -rf state/
    rm -rf logs/
    rm -f gridbot.log
    rm -f *.log
    
    # Remove .env (you'll need to recreate this)
    echo "🗑️  Removing .env file..."
    rm -f .env
    
    # Clean git (keep the repo)
    echo "🧹 Cleaning git..."
    git clean -fdx
    git reset --hard HEAD
    
    echo "✅ Cleanup complete in $BOT_DIR"
else
    echo "⚠️  Bot directory not found at $BOT_DIR"
fi

# Optional: Clean Docker containers/images if you used Docker
if command -v docker &> /dev/null; then
    echo "🐳 Cleaning Docker (if any)..."
    docker stop $(docker ps -aq --filter "name=gridbot") 2>/dev/null || true
    docker rm $(docker ps -aq --filter "name=gridbot") 2>/dev/null || true
fi

# Clean monitoring if installed
if systemctl is-active --quiet prometheus; then
    echo "📊 Stopping Prometheus..."
    sudo systemctl stop prometheus || true
fi

if systemctl is-active --quiet grafana-server; then
    echo "📊 Stopping Grafana..."
    sudo systemctl stop grafana-server || true
fi

echo ""
echo "✅ VPS cleaned successfully!"
echo ""
echo "Next steps:"
echo "1. Run: ./fresh_install.sh"
echo "2. Copy your .env file back"
echo "3. Start the bot"
