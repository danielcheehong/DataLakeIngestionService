# Data Lake Ingestion Service - Windows Installation Script
# Run this script as Administrator

param(
    [string]$ServiceName = "DataLakeIngestionService",
    [string]$BinaryPath = "$PSScriptRoot\..\src\Worker\bin\Release\net8.0\publish\DataLakeIngestionService.Worker.exe",
    [string]$ServiceAccount = "NT AUTHORITY\NETWORK SERVICE",
    [string]$ServicePassword = ""
)

Write-Host "Installing Data Lake Ingestion Service on Windows..." -ForegroundColor Green

# Check if running as Administrator
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "ERROR: This script must be run as Administrator" -ForegroundColor Red
    exit 1
}

# Check if binary exists
if (-not (Test-Path $BinaryPath)) {
    Write-Host "ERROR: Binary not found at: $BinaryPath" -ForegroundColor Red
    Write-Host "Please build and publish the project first:" -ForegroundColor Yellow
    Write-Host "  dotnet publish -c Release" -ForegroundColor Yellow
    exit 1
}

# Stop and remove existing service if it exists
$existingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existingService) {
    Write-Host "Stopping existing service..." -ForegroundColor Yellow
    Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
    
    Write-Host "Removing existing service..." -ForegroundColor Yellow
    sc.exe delete $ServiceName
    Start-Sleep -Seconds 2
}

# Create the service
Write-Host "Creating Windows Service..." -ForegroundColor Green
sc.exe create $ServiceName binPath= $BinaryPath start= auto DisplayName= "Data Lake Ingestion Service"

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to create service" -ForegroundColor Red
    exit 1
}

# Set service description
sc.exe description $ServiceName "ETL service for extracting data from SQL Server and Oracle, transforming, and uploading to data lake"

# Configure service account if specified
if ($ServiceAccount -ne "NT AUTHORITY\NETWORK SERVICE") {
    Write-Host "Configuring service account: $ServiceAccount" -ForegroundColor Green
    if ($ServicePassword) {
        sc.exe config $ServiceName obj= $ServiceAccount password= $ServicePassword
    } else {
        Write-Host "ERROR: Password required for custom service account" -ForegroundColor Red
        exit 1
    }
}

# Configure service recovery options
Write-Host "Configuring service recovery options..." -ForegroundColor Green
sc.exe failure $ServiceName reset= 86400 actions= restart/60000/restart/60000/restart/60000

# Create data directories
$dataLakePath = "C:\DataLake\Output"
if (-not (Test-Path $dataLakePath)) {
    Write-Host "Creating data directory: $dataLakePath" -ForegroundColor Green
    New-Item -ItemType Directory -Path $dataLakePath -Force | Out-Null
}

# Start the service
Write-Host "Starting service..." -ForegroundColor Green
Start-Service -Name $ServiceName

# Check service status
$service = Get-Service -Name $ServiceName
if ($service.Status -eq 'Running') {
    Write-Host "SUCCESS: Service installed and started successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Service Name: $ServiceName" -ForegroundColor Cyan
    Write-Host "Status: $($service.Status)" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "To manage the service:" -ForegroundColor Yellow
    Write-Host "  Start:   Start-Service -Name $ServiceName" -ForegroundColor Gray
    Write-Host "  Stop:    Stop-Service -Name $ServiceName" -ForegroundColor Gray
    Write-Host "  Restart: Restart-Service -Name $ServiceName" -ForegroundColor Gray
    Write-Host "  Status:  Get-Service -Name $ServiceName" -ForegroundColor Gray
} else {
    Write-Host "WARNING: Service installed but not running. Status: $($service.Status)" -ForegroundColor Yellow
    Write-Host "Check logs at: $PSScriptRoot\..\src\Worker\logs\" -ForegroundColor Yellow
}
