# VPS Deployment Guide

## Quick Start (5 minutes)

### Option A: Git Clone (Recommended)

```bash
# 1. SSH into your VPS
ssh user@your-vps-ip

# 2. Clone the repo
cd /opt
sudo git clone https://github.com/Looperbook/grid.git gridbot
cd gridbot

# 3. Run the installer
sudo bash deploy/install.sh

# 4. Create your .env file
sudo nano /opt/gridbot/.env
```

Add to `.env`:
```
HL_AGENT_KEY=0x377f751552adfe69811632c60f591e1ae190250bfc624f029a428f135172deb1
HL_USER_ADDRESS=0x78066857747283Fa7B5b205255Dbde4362cB481D
HL_BASE_URL=https://api.hyperliquid.xyz
```

```bash
# 5. Start the bot
sudo systemctl start gridbot

# 6. Check it's running
sudo systemctl status gridbot
sudo journalctl -u gridbot -f
```

---

### Option B: SCP Upload (If git unavailable)

From your **local machine** (Windows PowerShell):

```powershell
# Create a zip of the project (excluding .venv)
cd C:\Users\moham\OneDrive\Dokument\GitHub\grid
$exclude = @('.venv', '.git', '__pycache__', '.pytest_cache')
Compress-Archive -Path * -DestinationPath gridbot.zip -Force

# Upload to VPS
scp gridbot.zip user@your-vps-ip:/tmp/
```

On your **VPS**:
```bash
cd /opt
sudo unzip /tmp/gridbot.zip -d gridbot
cd gridbot
sudo bash deploy/install.sh
```

---

## Management Commands

```bash
# Start bot
sudo systemctl start gridbot

# Stop bot
sudo systemctl stop gridbot

# Restart bot
sudo systemctl restart gridbot

# Check status
sudo systemctl status gridbot

# View live logs
sudo journalctl -u gridbot -f

# View bot logs
sudo tail -f /var/log/gridbot/gridbot.log

# Edit configuration
sudo nano /opt/gridbot/configs/per_coin.yaml
sudo systemctl restart gridbot
```

---

## File Locations

| What | Path |
|------|------|
| Bot code | `/opt/gridbot/` |
| Configuration | `/opt/gridbot/configs/per_coin.yaml` |
| Environment | `/opt/gridbot/.env` |
| State files | `/opt/gridbot/state/` |
| Systemd logs | `journalctl -u gridbot` |
| Bot logs | `/var/log/gridbot/gridbot.log` |

---

## Monitoring

### Check if bot is running
```bash
systemctl is-active gridbot
```

### Check resource usage
```bash
systemctl status gridbot
# Shows memory, CPU, uptime
```

### Check for errors
```bash
sudo journalctl -u gridbot --since "1 hour ago" | grep -i error
```

---

## Troubleshooting

### Bot won't start
```bash
# Check logs for errors
sudo journalctl -u gridbot -n 50

# Test manually
cd /opt/gridbot
sudo -u gridbot .venv/bin/python start_bot.py
```

### Permission errors
```bash
sudo chown -R gridbot:gridbot /opt/gridbot
sudo chmod 600 /opt/gridbot/.env
```

### Python version issues
```bash
# Check Python version
python3 --version

# Install Python 3.11 if needed
sudo apt install python3.11 python3.11-venv
```

---

## Security Notes

1. **Never commit `.env`** - It contains your private keys
2. **Use agent keys** - Don't use your main wallet private key
3. **Firewall** - The bot doesn't need any open ports (outbound only)
4. **Updates** - Pull latest code and restart:
   ```bash
   cd /opt/gridbot
   sudo -u gridbot git pull
   sudo systemctl restart gridbot
   ```

---

## Auto-restart on crash

The systemd service is configured to:
- Restart automatically after 10 seconds on crash
- Start automatically on boot
- Use limited resources (512MB RAM, 50% CPU max)
