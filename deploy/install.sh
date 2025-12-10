#!/bin/bash
# VPS Deployment Script for Grid Trading Bot
# Run as root or with sudo

set -e

echo "=========================================="
echo "Grid Trading Bot - VPS Deployment"
echo "=========================================="

# Configuration
BOT_USER="gridbot"
BOT_DIR="/opt/gridbot"
LOG_DIR="/var/log/gridbot"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root or with sudo"
    exit 1
fi

echo "[1/8] Installing system dependencies..."
apt-get update

# Try to find available Python version (3.11, 3.10, or 3.9)
if apt-cache show python3.11 &>/dev/null; then
    PYTHON_VERSION="3.11"
elif apt-cache show python3.10 &>/dev/null; then
    PYTHON_VERSION="3.10"
elif apt-cache show python3.9 &>/dev/null; then
    PYTHON_VERSION="3.9"
else
    # Fallback to default python3
    PYTHON_VERSION="3"
fi

echo "Using Python version: $PYTHON_VERSION"
apt-get install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-venv python${PYTHON_VERSION}-dev git curl || \
apt-get install -y python3 python3-venv python3-dev git curl

echo "[2/8] Creating bot user..."
if ! id "$BOT_USER" &>/dev/null; then
    useradd --system --shell /bin/false --home-dir $BOT_DIR $BOT_USER
    echo "Created user: $BOT_USER"
else
    echo "User $BOT_USER already exists"
fi

echo "[3/8] Creating directories..."
mkdir -p $BOT_DIR
mkdir -p $BOT_DIR/state
mkdir -p $BOT_DIR/logs
mkdir -p $LOG_DIR
chown -R $BOT_USER:$BOT_USER $BOT_DIR
chown -R $BOT_USER:$BOT_USER $LOG_DIR

echo "[4/8] Copying bot files..."
# Copy all necessary files (run this from the repo directory)
cp -r src $BOT_DIR/
cp -r configs $BOT_DIR/
cp requirements.txt $BOT_DIR/
cp start_bot.py $BOT_DIR/
cp pytest.ini $BOT_DIR/

# Copy state files if they exist
if [ -d "state" ] && [ "$(ls -A state)" ]; then
    cp -r state/* $BOT_DIR/state/
fi

echo "[5/8] Setting up Python virtual environment..."
# Use the detected Python version or fall back to python3
PYTHON_CMD="python${PYTHON_VERSION}"
if ! command -v $PYTHON_CMD &>/dev/null; then
    PYTHON_CMD="python3"
fi
echo "Using: $PYTHON_CMD"
sudo -u $BOT_USER $PYTHON_CMD -m venv $BOT_DIR/.venv
sudo -u $BOT_USER $BOT_DIR/.venv/bin/pip install --upgrade pip
sudo -u $BOT_USER $BOT_DIR/.venv/bin/pip install -r $BOT_DIR/requirements.txt

echo "[6/8] Setting up environment file..."
if [ ! -f "$BOT_DIR/.env" ]; then
    echo "⚠️  No .env file found!"
    echo "   Please create $BOT_DIR/.env with your configuration:"
    echo ""
    echo "   HL_AGENT_KEY=0x..."
    echo "   HL_USER_ADDRESS=0x..."
    echo "   HL_BASE_URL=https://api.hyperliquid.xyz"
    echo ""
    read -p "Press Enter to continue after creating .env file..."
fi
chmod 600 $BOT_DIR/.env 2>/dev/null || true
chown $BOT_USER:$BOT_USER $BOT_DIR/.env 2>/dev/null || true

echo "[7/8] Installing systemd service..."
cp deploy/gridbot.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable gridbot

echo "[8/8] Setting permissions..."
chown -R $BOT_USER:$BOT_USER $BOT_DIR

echo ""
echo "=========================================="
echo "✅ Deployment complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Create/edit .env file:  sudo nano $BOT_DIR/.env"
echo "  2. Start the bot:          sudo systemctl start gridbot"
echo "  3. Check status:           sudo systemctl status gridbot"
echo "  4. View logs:              sudo journalctl -u gridbot -f"
echo "  5. View bot logs:          sudo tail -f $LOG_DIR/gridbot.log"
echo ""
echo "Management commands:"
echo "  - Stop:     sudo systemctl stop gridbot"
echo "  - Restart:  sudo systemctl restart gridbot"
echo "  - Disable:  sudo systemctl disable gridbot"
echo ""
