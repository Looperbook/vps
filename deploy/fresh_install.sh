#!/bin/bash
#
# Fresh VPS Installation Script
# Installs gridbot from scratch after cleanup
#
# Usage: ./fresh_install.sh
#

set -e

echo "🚀 Fresh GridBot Installation Starting..."

# Configuration
BOT_DIR="/root/grid"
PYTHON_VERSION="python3.11"

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "❌ Error: Must run from bot directory (should contain requirements.txt)"
    exit 1
fi

# Update system packages
echo "📦 Updating system packages..."
sudo apt-get update -qq

# Install Python and dependencies
echo "🐍 Installing Python $PYTHON_VERSION..."
sudo apt-get install -y python3.11 python3.11-venv python3-pip git

# Pull latest code
echo "📥 Pulling latest code from GitHub..."
git fetch origin
git reset --hard origin/main
git pull origin main

# Create virtual environment
echo "🔧 Creating virtual environment..."
$PYTHON_VERSION -m venv .venv

# Activate and install dependencies
echo "📦 Installing Python packages..."
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p state/wal
mkdir -p logs

# Check for .env file
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found!"
    echo "📝 Creating .env template from .env.example..."
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "✅ Created .env - EDIT THIS FILE with your credentials!"
    else
        echo "❌ No .env.example found. You need to create .env manually."
    fi
else
    echo "✅ .env file exists"
fi

# Install systemd service
echo "⚙️  Installing systemd service..."
sudo cp deploy/gridbot.service /etc/systemd/system/
sudo systemctl daemon-reload

# Set permissions
echo "🔐 Setting permissions..."
chmod +x start_bot.py check_orders.py cancel_all.py

echo ""
echo "✅ Fresh installation complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file:     nano .env"
echo "2. Test the bot:       python start_bot.py"
echo "3. Enable service:     sudo systemctl enable gridbot"
echo "4. Start service:      sudo systemctl start gridbot"
echo "5. Check logs:         sudo journalctl -u gridbot -f"
echo ""
echo "Optional - Install monitoring:"
echo "  cd deploy/monitoring && ./install_monitoring.sh"
