#!/bin/bash
# Monitoring Stack Installation Script
# Installs Prometheus + Grafana for GridBot metrics
# Run as root or with sudo

set -e

echo "=========================================="
echo "GridBot Monitoring Stack Installation"
echo "Prometheus + Grafana"
echo "=========================================="

# Configuration
PROMETHEUS_VERSION="2.47.2"
NODE_EXPORTER_VERSION="1.6.1"
GRAFANA_PORT=3000
PROMETHEUS_PORT=9091
BOT_METRICS_PORT=9090

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root or with sudo"
    exit 1
fi

echo "[1/7] Installing dependencies..."
apt-get update
apt-get install -y wget curl apt-transport-https software-properties-common

echo "[2/7] Installing Prometheus..."
cd /tmp

# Download Prometheus
wget -q https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz
tar xvfz prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz
cd prometheus-${PROMETHEUS_VERSION}.linux-amd64

# Create prometheus user
useradd --no-create-home --shell /bin/false prometheus 2>/dev/null || true

# Create directories
mkdir -p /etc/prometheus
mkdir -p /var/lib/prometheus

# Copy binaries
cp prometheus /usr/local/bin/
cp promtool /usr/local/bin/
cp -r consoles /etc/prometheus/
cp -r console_libraries /etc/prometheus/

# Set ownership
chown prometheus:prometheus /usr/local/bin/prometheus
chown prometheus:prometheus /usr/local/bin/promtool
chown -R prometheus:prometheus /etc/prometheus
chown -R prometheus:prometheus /var/lib/prometheus

echo "[3/7] Configuring Prometheus..."
cat > /etc/prometheus/prometheus.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

alerting:
  alertmanagers:
    - static_configs:
        - targets: []

rule_files: []

scrape_configs:
  # Prometheus self-monitoring
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9091']

  # GridBot metrics
  - job_name: 'gridbot'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 5s
    metrics_path: /metrics

  # Node Exporter (system metrics)
  - job_name: 'node'
    static_configs:
      - targets: ['localhost:9100']
EOF

chown prometheus:prometheus /etc/prometheus/prometheus.yml

echo "[4/7] Creating Prometheus systemd service..."
cat > /etc/systemd/system/prometheus.service << EOF
[Unit]
Description=Prometheus
Wants=network-online.target
After=network-online.target

[Service]
User=prometheus
Group=prometheus
Type=simple
ExecStart=/usr/local/bin/prometheus \\
    --config.file=/etc/prometheus/prometheus.yml \\
    --storage.tsdb.path=/var/lib/prometheus/ \\
    --web.console.templates=/etc/prometheus/consoles \\
    --web.console.libraries=/etc/prometheus/console_libraries \\
    --web.listen-address=:${PROMETHEUS_PORT} \\
    --storage.tsdb.retention.time=30d

Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

echo "[5/7] Installing Node Exporter..."
cd /tmp
wget -q https://github.com/prometheus/node_exporter/releases/download/v${NODE_EXPORTER_VERSION}/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
tar xvfz node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
cp node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64/node_exporter /usr/local/bin/
useradd --no-create-home --shell /bin/false node_exporter 2>/dev/null || true
chown node_exporter:node_exporter /usr/local/bin/node_exporter

cat > /etc/systemd/system/node_exporter.service << EOF
[Unit]
Description=Node Exporter
Wants=network-online.target
After=network-online.target

[Service]
User=node_exporter
Group=node_exporter
Type=simple
ExecStart=/usr/local/bin/node_exporter

Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

echo "[6/7] Installing Grafana..."
# Add Grafana repository
wget -q -O /usr/share/keyrings/grafana.key https://apt.grafana.com/gpg.key
echo "deb [signed-by=/usr/share/keyrings/grafana.key] https://apt.grafana.com stable main" | tee /etc/apt/sources.list.d/grafana.list
apt-get update
apt-get install -y grafana

# Configure Grafana
cat > /etc/grafana/provisioning/datasources/prometheus.yml << EOF
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://localhost:${PROMETHEUS_PORT}
    isDefault: true
    editable: false
EOF

echo "[7/7] Starting services..."
systemctl daemon-reload

# Enable and start all services
systemctl enable prometheus
systemctl enable node_exporter
systemctl enable grafana-server

systemctl start prometheus
systemctl start node_exporter
systemctl start grafana-server

# Wait for services to start
sleep 5

# Cleanup
cd /tmp
rm -rf prometheus-* node_exporter-*

echo ""
echo "=========================================="
echo "Installation Complete!"
echo "=========================================="
echo ""
echo "Services running:"
echo "  • Prometheus:    http://localhost:${PROMETHEUS_PORT}"
echo "  • Grafana:       http://localhost:${GRAFANA_PORT}"
echo "  • Node Exporter: http://localhost:9100/metrics"
echo "  • GridBot:       http://localhost:${BOT_METRICS_PORT}/metrics"
echo ""
echo "Grafana Login:"
echo "  • URL:      http://YOUR_VPS_IP:${GRAFANA_PORT}"
echo "  • Username: admin"
echo "  • Password: admin (change on first login)"
echo ""
echo "Next steps:"
echo "  1. Open Grafana in browser: http://YOUR_VPS_IP:${GRAFANA_PORT}"
echo "  2. Login with admin/admin"
echo "  3. Import dashboard: Deploy > monitoring > gridbot_dashboard.json"
echo ""
echo "To check status:"
echo "  systemctl status prometheus"
echo "  systemctl status grafana-server"
echo "  systemctl status node_exporter"
echo ""
