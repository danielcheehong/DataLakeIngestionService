#!/bin/bash
# Data Lake Ingestion Service - Linux Installation Script
# Run this script with sudo

SERVICE_NAME="datalake-ingestion"
SERVICE_USER="datalake"
INSTALL_DIR="/opt/datalake-ingestion"
DATA_DIR="/var/datalake/output"
LOG_DIR="/var/log/datalake-ingestion"
BINARY_PATH="../src/Worker/bin/Release/net8.0/publish"

echo "Installing Data Lake Ingestion Service on Linux..."

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "ERROR: This script must be run as root (use sudo)"
    exit 1
fi

# Check if .NET 8 is installed
if ! command -v dotnet &> /dev/null; then
    echo "ERROR: .NET 8 SDK/Runtime not found"
    echo "Please install .NET 8 first:"
    echo "  https://learn.microsoft.com/en-us/dotnet/core/install/linux"
    exit 1
fi

# Check if binary exists
if [ ! -d "$BINARY_PATH" ]; then
    echo "ERROR: Binary not found at: $BINARY_PATH"
    echo "Please build and publish the project first:"
    echo "  dotnet publish -c Release"
    exit 1
fi

# Create service user
if ! id "$SERVICE_USER" &>/dev/null; then
    echo "Creating service user: $SERVICE_USER"
    useradd --system --no-create-home --shell /bin/false $SERVICE_USER
fi

# Create directories
echo "Creating application directories..."
mkdir -p $INSTALL_DIR
mkdir -p $DATA_DIR
mkdir -p $LOG_DIR

# Copy binaries
echo "Copying application files..."
cp -r $BINARY_PATH/* $INSTALL_DIR/

# Set permissions
echo "Setting permissions..."
chown -R $SERVICE_USER:$SERVICE_USER $INSTALL_DIR
chown -R $SERVICE_USER:$SERVICE_USER $DATA_DIR
chown -R $SERVICE_USER:$SERVICE_USER $LOG_DIR
chmod +x $INSTALL_DIR/DataLakeIngestionService.Worker

# Create systemd service file
echo "Creating systemd service..."
cat > /etc/systemd/system/${SERVICE_NAME}.service << EOF
[Unit]
Description=Data Lake Ingestion Service
After=network.target

[Service]
Type=notify
User=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/DataLakeIngestionService.Worker
Restart=always
RestartSec=10
KillSignal=SIGINT
SyslogIdentifier=datalake-ingestion
PrivateTmp=true

# Environment variables
Environment=DOTNET_ENVIRONMENT=Production
Environment=ASPNETCORE_ENVIRONMENT=Production

# Resource limits
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

# Update appsettings for Linux paths
if [ -f "$INSTALL_DIR/appsettings.json" ]; then
    echo "Updating configuration for Linux..."
    sed -i 's|C:\\\\DataLake\\\\Output|/var/datalake/output|g' $INSTALL_DIR/appsettings.json
    sed -i 's|logs/datalake-|/var/log/datalake-ingestion/datalake-|g' $INSTALL_DIR/appsettings.json
fi

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

# Enable service
echo "Enabling service..."
systemctl enable ${SERVICE_NAME}.service

# Start service
echo "Starting service..."
systemctl start ${SERVICE_NAME}.service

# Check status
sleep 2
if systemctl is-active --quiet ${SERVICE_NAME}.service; then
    echo ""
    echo "SUCCESS: Service installed and started successfully!"
    echo ""
    echo "Service Name: ${SERVICE_NAME}"
    echo "Status: $(systemctl is-active ${SERVICE_NAME}.service)"
    echo ""
    echo "To manage the service:"
    echo "  Start:   sudo systemctl start ${SERVICE_NAME}"
    echo "  Stop:    sudo systemctl stop ${SERVICE_NAME}"
    echo "  Restart: sudo systemctl restart ${SERVICE_NAME}"
    echo "  Status:  sudo systemctl status ${SERVICE_NAME}"
    echo "  Logs:    sudo journalctl -u ${SERVICE_NAME} -f"
else
    echo ""
    echo "WARNING: Service installed but not running"
    echo "Check logs with: sudo journalctl -u ${SERVICE_NAME} -n 50"
fi

echo ""
echo "Data directory: $DATA_DIR"
echo "Log directory: $LOG_DIR"
