# Data Lake Ingestion Service - Windows Uninstall Script
# Run this script as Administrator

param(
    [string]$ServiceName = "DataLakeIngestionService"
)

Write-Host "Uninstalling Data Lake Ingestion Service..." -ForegroundColor Yellow

# Check if running as Administrator
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "ERROR: This script must be run as Administrator" -ForegroundColor Red
    exit 1
}

# Check if service exists
$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if (-not $service) {
    Write-Host "Service not found: $ServiceName" -ForegroundColor Yellow
    exit 0
}

# Stop the service
Write-Host "Stopping service..." -ForegroundColor Yellow
Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue

# Remove the service
Write-Host "Removing service..." -ForegroundColor Yellow
sc.exe delete $ServiceName

if ($LASTEXITCODE -eq 0) {
    Write-Host "SUCCESS: Service uninstalled successfully!" -ForegroundColor Green
} else {
    Write-Host "ERROR: Failed to uninstall service" -ForegroundColor Red
    exit 1
}
