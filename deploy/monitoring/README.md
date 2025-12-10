# Monitoring Setup Guide

## Quick Install (One Command)

SSH into your VPS and run:

```bash
cd /opt/gridbot
sudo bash deploy/monitoring/install_monitoring.sh
```

This installs:
- **Prometheus** (port 9091) - Time-series database
- **Grafana** (port 3000) - Dashboard UI
- **Node Exporter** (port 9100) - System metrics

## Access Grafana

1. Open in browser: `http://YOUR_VPS_IP:3000`
2. Login: `admin` / `admin` (change password on first login)
3. Prometheus datasource is pre-configured

## Import Dashboard

1. Go to **Dashboards** → **Import**
2. Upload `deploy/monitoring/gridbot_dashboard.json`
3. Or paste the JSON content directly
4. Click **Import**

## Firewall Configuration

Open required ports:

```bash
# UFW (Ubuntu)
sudo ufw allow 3000/tcp   # Grafana
sudo ufw allow 9091/tcp   # Prometheus (optional, for external access)

# Or with iptables
sudo iptables -A INPUT -p tcp --dport 3000 -j ACCEPT
```

## Verify Setup

Check all services are running:

```bash
systemctl status prometheus
systemctl status grafana-server
systemctl status node_exporter
systemctl status gridbot
```

Test metrics endpoints:

```bash
curl localhost:9090/metrics  # GridBot metrics
curl localhost:9091/metrics  # Prometheus metrics
curl localhost:9100/metrics  # Node exporter metrics
```

## Dashboard Panels

The GridBot dashboard includes:

### Overview Row
- **Position** - Current position size
- **Session PnL** - Profit/Loss this session
- **Daily PnL** - Today's P/L
- **Active Orders** - Current resting orders
- **Risk Status** - OK or HALTED
- **API Error Streak** - Consecutive errors

### Position & PnL Row
- Position over time chart
- PnL trends (session & daily)

### Order Activity Row
- Fill rate (buys/sells per minute)
- Order lifecycle (submitted, cancelled, active)

### Grid Health Row
- Grid width percentage
- Skew ratio
- Price vs Grid Center

### System Health Row
- Position drift monitoring
- API errors and WS gaps
- Shadow ledger alerts

### System Resources Row
- CPU usage
- Memory usage

## Alerting (Optional)

Add alerts in Grafana for:

1. **Risk Halted**: `gridbot_risk_halted == 1`
2. **Position Drift**: `gridbot_position_drift_amount > 0.05`
3. **API Errors**: `gridbot_api_error_streak > 5`
4. **No Fills**: `rate(gridbot_fills_total[10m]) == 0`

## Troubleshooting

### Grafana can't reach Prometheus
```bash
# Check Prometheus is running
curl localhost:9091/api/v1/targets

# Check GridBot metrics
curl localhost:9090/metrics | grep gridbot
```

### No GridBot metrics
```bash
# Ensure GridBot is exposing metrics
curl localhost:9090/metrics

# Check GridBot status
systemctl status gridbot
journalctl -u gridbot -n 50
```

### Dashboard shows "No data"
1. Verify time range (top right)
2. Check Prometheus datasource in panel
3. Ensure metrics are being scraped: Prometheus → Status → Targets
