# Data Lake Ingestion Service

A cross-platform .NET 8 background service for extracting data from SQL Server and Oracle databases, applying transformations, generating Parquet files, and uploading to various storage destinations.

## Features

- **Multi-Database Support**: Extract from SQL Server (stored procedures) and Oracle (packages with REF CURSORs)
- **Chain of Responsibility Pipeline**: Modular extraction → transformation → Parquet generation → upload pipeline
- **Dynamic Transformation Discovery**: Auto-discovers transformation steps via reflection at startup
- **Scheduled Execution**: Quartz.NET with cron expressions for flexible scheduling
- **Cross-Platform**: Runs on Windows (as Windows Service) and Linux (as systemd daemon)
- **Multiple Upload Targets**: FileSystem, Azure Blob Storage (AWS S3 and Axway ready for implementation)
- **Parquet Generation**: Columnar storage with Snappy compression
- **Independent Task Execution**: Each dataset runs as an independent Quartz job with concurrent execution control
- **Dapper Integration**: Lightweight ORM for efficient data extraction
- **Structured Logging**: Serilog with console and file outputs

## Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────┐
│              Quartz.NET Scheduler (In-Memory)               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Dataset Job  │  │ Dataset Job  │  │ Dataset Job  │     │
│  │   (Cron)     │  │   (Cron)     │  │   (Cron)     │     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
└─────────┼──────────────────┼──────────────────┼─────────────┘
          │                  │                  │
          └──────────────────┴──────────────────┘
                             │
        ┌────────────────────▼────────────────────┐
        │    DataIngestionJob (IJob)              │
        │  - Loads dataset configuration          │
        │  - Resolves connection string           │
        │  - Generates file name from pattern     │
        │  - Builds pipeline metadata             │
        │  - Executes DataPipeline                │
        └────────────────────┬────────────────────┘
                             │
        ┌────────────────────▼────────────────────┐
        │  Chain of Responsibility Pipeline       │
        ├─────────────────────────────────────────┤
        │ 1. ExtractionHandler                    │
        │    ├─ DataSourceFactory                 │
        │    ├─ SqlServerDataSource (Dapper)      │
        │    └─ OracleDataSource (REF CURSOR)     │
        ├─────────────────────────────────────────┤
        │ 2. TransformationHandler                │
        │    ├─ TransformationStepFactory (Auto-  │
        │    │  discovery via Reflection)          │
        │    ├─ TransformationEngine               │
        │    ├─ DataCleansingStep                 │
        │    └─ DataValidationStep                │
        ├─────────────────────────────────────────┤
        │ 3. ParquetGenerationHandler             │
        │    └─ ParquetWriterService              │
        │       (Schema inference, Snappy)        │
        ├─────────────────────────────────────────┤
        │ 4. UploadHandler                        │
        │    ├─ UploadProviderFactory             │
        │    ├─ FileSystemUploadProvider          │
        │    └─ AzureBlobStorageProvider          │
        └─────────────────────────────────────────┘
        
        Supporting Services:
        ├─ ConnectionStringBuilder
        ├─ DatasetConfigurationService
        ├─ JobSchedulingService
        └─ Vault Services (EvaVaultService, SecuritasVaultService)
```

### Pipeline Context Flow

```
PipelineContext (Job Metadata)
    │
    ├─► ExtractedData (DataTable)
    ├─► TransformedData (DataTable)  
    ├─► ParquetStream (MemoryStream)
    └─► UploadUri (string)
```

## Prerequisites

- **.NET 8 SDK** (or later)
- **SQL Server** (for SQL Server data sources)
- **Oracle Instant Client** (for Oracle data sources with REF CURSOR support)
- **Windows 10/Server 2016+** or **Linux** (Ubuntu 20.04+, RHEL 8+, Debian 10+)
- **Visual Studio 2022** or **VS Code** (optional, for development)

## Key Technologies

| Technology | Version | Purpose |
|-----------|---------|---------|
| .NET | 8.0 | Cross-platform runtime |
| Dapper | 2.1.28 | Lightweight ORM |
| Quartz.NET | 3.8.0 | Job scheduling |
| Parquet.Net | 4.18.1 | Parquet file generation |
| Serilog | 3.1.1 | Structured logging |
| Oracle.ManagedDataAccess.Core | 3.21.130 | Oracle connectivity |
| Microsoft.Data.SqlClient | 5.1.5 | SQL Server connectivity |
| Azure.Storage.Blobs | 12.19.1 | Azure Blob Storage |

## Quick Start

### 1. Clone and Build

```bash
# Clone repository
git clone https://github.com/danielcheehong/DataLakeIngestionService.git
cd DataLakeIngestionService

# Restore dependencies
dotnet restore

# Build solution
dotnet build

# Publish for deployment
dotnet publish src/Worker/DataLakeIngestionService.Worker.csproj -c Release -o ./publish
```

### 2. Configure Connection Strings

Edit `src/Worker/appsettings.json`:

```json
{
  "Datasets": {
    "ConfigurationPath": "./Datasets",
    "EnableHotReload": true,
    "ReloadIntervalSeconds": 300
  },

  "ConnectionStrings": {
    "TradesSqlServer": "Server=localhost;Database=Trades;User Id=sa;Password=TestPwd123!;TrustServerCertificate=true;Encrypt=false;",
    "HROracleDB": "Data Source=localhost:1521/XEPDB1;User Id=hr_service;Password=ServicePassword123!;"
  },
  
  "FileSystemProvider": {
    "BasePath": "C:\\temp\\DataLake",  // Windows
    // "BasePath": "/var/datalake/output",  // Linux
    "MaxRetries": 3
  },
  
  "AzureBlob": {
    "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;",
    "DefaultContainer": "raw-data"
  },

  "VaultConfiguration": {
    "Provider": "securitas",
    "BaseUrl": "https://vault.example.com/api/v1",
    "AuthToken": "",
    "ApiKey": ""
  }
}
```

### 2a. Secure Credential Management with Vault Placeholders

The service supports secure credential management through vault integration. Instead of storing passwords directly in configuration files, use **vault placeholders** that are resolved at runtime.

#### Vault Placeholder Syntax

Use the following syntax in connection strings to reference secrets stored in vault systems:

```
{vault:path/to/secret}
```

**Example - SQL Server with Vault:**
```json
"ConnectionStrings": {
  "TradesSqlServer": "Server=prod-sql.company.com;Database=Trades;User Id=sa;Password={vault:production/sqlserver/trades_password};TrustServerCertificate=true;"
}
```

**Example - Oracle with Vault:**
```json
"ConnectionStrings": {
  "HROracleDB": "Data Source=prod-oracle:1521/PROD;User Id=hr_service;Password={vault:production/oracle/hr_password};"
}
```

#### How Vault Resolution Works

1. **Configuration**: Connection strings contain `{vault:...}` placeholders
2. **Runtime Detection**: `ConnectionStringBuilder` uses regex pattern `\{vault:([^}]+)\}` to detect placeholders
3. **Vault Lookup**: Configured vault service (EVA or Securitas) retrieves the actual secret
4. **Caching**: Retrieved secrets are cached in memory for 5 minutes to reduce vault API calls
5. **Connection**: The resolved connection string (with actual password) is used for database connections

#### Vault Configuration

Configure vault provider settings in `appsettings.json`:

```json
{
  "VaultConfiguration": {
    "Provider": "securitas",
    "BaseUrl": "https://vault.company.com/api/v1",
    "AuthToken": "${VAULT_AUTH_TOKEN}",
    "ApiKey": "${VAULT_API_KEY}"
  }
}
```

**Configuration Properties:**

| Property | Required | Description | Example |
|----------|----------|-------------|---------||
| `Provider` | Yes | Vault service provider | `"securitas"` or `"eva"` |
| `BaseUrl` | Yes | Vault API endpoint | `"https://vault.example.com/api/v1"` |
| `AuthToken` | For Securitas | Bearer token for authentication | `"${VAULT_AUTH_TOKEN}"` (from env var) |
| `ApiKey` | For EVA | API key for authentication | `"${VAULT_API_KEY}"` (from env var) |

**Available Vault Providers:**

| Provider | Implementation | Authentication Method |
|----------|----------------|----------------------|
| `securitas` | `SecuritasVaultService` | Bearer token (Authorization header) |
| `eva` | `EvaVaultService` | API key (X-API-Key header) |

#### Environment-Specific Configuration

**Development (Local Testing):**

For local development without vault access, use **plaintext passwords without placeholders**:

```json
// appsettings.Development.json
{
  "ConnectionStrings": {
    "TradesSqlServer": "Server=localhost;Database=Trades;User Id=sa;Password=LocalDevPassword123!;",
    "HROracleDB": "Data Source=localhost:1521/XEPDB1;User Id=hr_service;Password=ServicePassword123!;"
  }
}
```

When no `{vault:...}` placeholders are detected, `ConnectionStringBuilder` returns the connection string unchanged with **no vault service calls**.

**Staging:**

```json
// appsettings.Staging.json
{
  "VaultConfiguration": {
    "Provider": "eva",
    "BaseUrl": "https://vault.staging.example.com/api/v1",
    "AuthToken": "${VAULT_AUTH_TOKEN}"
  },
  "ConnectionStrings": {
    "TradesSqlServer": "Server=staging-sql.company.com;Database=Trades;User Id=app_user;Password={vault:staging/sqlserver/trades_password};",
    "HROracleDB": "Data Source=staging-oracle:1521/STAGE;User Id=hr_service;Password={vault:staging/oracle/hr_password};"
  }
}
```

**Production:**

```json
// appsettings.Production.json
{
  "VaultConfiguration": {
    "Provider": "securitas",
    "BaseUrl": "https://vault.production.example.com/api/v1",
    "AuthToken": "${VAULT_AUTH_TOKEN}"
  },
  "ConnectionStrings": {
    "TradesSqlServer": "Server=prod-sql.company.com;Database=Trades;User Id=app_user;Password={vault:production/sqlserver/trades_password};",
    "HROracleDB": "Data Source=prod-oracle:1521/PROD;User Id=hr_service;Password={vault:production/oracle/hr_password};"
  }
}
```

#### Multiple Vault Placeholders

You can use multiple vault placeholders in a single connection string:

```json
"TradesSqlServer": "Server=localhost;Database=Trades;User Id={vault:sqlserver/trades_username};Password={vault:sqlserver/trades_password};"
```

All placeholders are independently resolved and replaced with their respective secrets.

#### Security Best Practices

✅ **DO:**
- Use vault placeholders for production and staging environments
- Store vault authentication tokens in environment variables (e.g., `${VAULT_AUTH_TOKEN}`)
- Use plaintext passwords only in local development (appsettings.Development.json)
- Keep development and production connection strings in separate environment-specific files
- Restrict file system permissions on appsettings files in production
- Use different vault paths for different environments (e.g., `production/`, `staging/`)

❌ **DON'T:**
- Commit plaintext production passwords to source control
- Use vault placeholders without configuring VaultConfiguration section
- Store vault auth tokens directly in configuration files (use environment variables)
- Use production credentials in development or staging environments
- Share vault paths between environments

#### Troubleshooting Vault Configuration

**Error: "Vault provider not configured"**

```log
[ERR] System.InvalidOperationException: Vault provider not configured
```

**Solution:** Add `VaultConfiguration` section to appsettings.json with `Provider` property set to either `"securitas"` or `"eva"`.

---

**Error: Vault placeholder not resolved**

```log
[WRN] Failed to resolve vault placeholder: production/sqlserver/password
```

**Possible causes:**
- Vault service authentication failed (check AuthToken/ApiKey)
- Vault path doesn't exist or has incorrect permissions
- Network connectivity issues to vault service
- Vault service BaseUrl is incorrect

**Solution:** 
- Verify vault credentials are set in environment variables
- Test vault API endpoint manually using curl or Postman
- Check vault service logs for permission errors
- Ensure firewall rules allow access to vault service

---

**Info: "No vault placeholders found"**

```log
[DBG] No vault placeholders found in connection string
```

**This is normal** when using plaintext passwords (typical in development). The connection string is used as-is without vault resolution.

---

**Example Log - Successful Vault Resolution:**

```log
[INF] Found 1 vault placeholders to resolve
[DBG] Resolving vault placeholder: production/sqlserver/trades_password
[DBG] Retrieved secret from vault (cached for 5 minutes)
[INF] Successfully resolved all vault placeholders
[INF] Executing SQL Server query: dbo.sp_GetDailyTrades
```

### 3. Create Dataset Configuration

Create a JSON file in `src/Worker/Datasets/` (e.g., `dataset-sales-sqlserver.json`):

```json
{
  "datasetId": "Trades-daily-sqlserver",
  "name": "Daily Trades Transactions",
  "description": "Extract daily Trades data from SQL Server",
  "enabled": true,
  "cronExpression": "0 */2 * * * ?",  // Every 2 minutes

  "source": {
    "type": "SqlServer",
    "connectionStringKey": "TradesSqlServer",
    "extractionType": "StoredProcedure",
    "procedureName": "dbo.sp_GetDailyTrades",
    "parameters": {
      "StartDate": "2024-01-01",
      "EndDate": "2025-12-31"
    },
    "commandTimeout": 300
  },

  "transformations": [],

  "parquet": {
    "fileNamePattern": "Trades_{date:yyyyMMdd}_{time:HHmmss}.parquet",
    "compressionCodec": "Snappy",
    "rowGroupSize": 10000,
    "enableStatistics": true
  },

  "upload": {
    "provider": "FileSystem",
    "fileSystemConfig": {
      "basePath": "",
      "relativePath": "Trades/Transcations/"
    },
    "overwriteExisting": false,
    "enableRetry": true,
    "maxRetries": 3
  },

  "notifications": {
    "onSuccess": false,
    "onFailure": true,
    "channels": ["email"]
  },

  "metadata": {
    "owner": "Trades Team",
    "contact": "Trades-team@company.com",
    "tags": ["Trades", "daily", "transactions"]
  }
}
```

### 4. Run Locally (Console Mode)

```bash
cd src/Worker
dotnet run

# Or run published version
cd publish
dotnet DataLakeIngestionService.Worker.dll

# Force console mode (not as service)
dotnet DataLakeIngestionService.Worker.dll --console
```

**Console Output:**
```
[INF] Starting Data Lake Ingestion Service
[INF] Configured for Windows Service hosting
[INF] Service configured successfully. Starting host...
[INF] Loaded 2 dataset configurations
[INF] Scheduled job for dataset: Trades-daily-sqlserver with cron: 0 */2 * * * ?
[INF] Scheduled job for dataset: hr-employees-oracle with cron: 0 */1 * * * ?
```

## Installation

### Windows Service

```powershell
# Run as Administrator
cd scripts
.\install-service.ps1
```

### Linux systemd

```bash
# Run with sudo
cd scripts
chmod +x install-daemon.sh
sudo ./install-daemon.sh
```

## Configuration Details

### Application Settings Overview

The service uses a hierarchical configuration system with environment-specific overrides:
- `appsettings.json` - Base configuration (committed to source control)
- `appsettings.Development.json` - Local development overrides
- `appsettings.Staging.json` - Staging environment overrides
- `appsettings.Production.json` - Production environment overrides

Environment-specific files override base settings and are typically excluded from source control to protect sensitive credentials.

### VaultConfiguration Section

Configure the vault service provider for secure credential management:

```json
{
  "VaultConfiguration": {
    "Provider": "securitas",
    "BaseUrl": "https://vault.example.com/api/v1",
    "AuthToken": "",
    "ApiKey": ""
  }
}
```

**Configuration Properties:**

| Property | Required | Description | Used By |
|----------|----------|-------------|---------|
| `Provider` | Yes | Vault service provider name (`"securitas"` or `"eva"`) | Both providers |
| `BaseUrl` | Yes | Base URL of the vault API endpoint | Both providers |
| `AuthToken` | Conditional | Bearer token for authentication | Securitas only |
| `ApiKey` | Conditional | API key for authentication | EVA only |

**Available Providers:**

| Provider Value | Implementation | Authentication Method | HTTP Header |
|----------------|----------------|----------------------|-------------|
| `"securitas"` | `SecuritasVaultService` | Bearer token | `Authorization: Bearer {token}` |
| `"eva"` | `EvaVaultService` | API key | `X-API-Key: {key}` |

**Environment-Specific Examples:**

```json
// Development - May use development vault or plaintext passwords
{
  "VaultConfiguration": {
    "Provider": "securitas",
    "BaseUrl": "https://vault.dev.example.com/api/v1",
    "AuthToken": "dev-token-123",
    "ApiKey": ""
  }
}

// Staging - EVA vault with environment variable
{
  "VaultConfiguration": {
    "Provider": "eva",
    "BaseUrl": "https://vault.staging.example.com/api/v1",
    "AuthToken": "",
    "ApiKey": "${VAULT_API_KEY}"
  }
}

// Production - Securitas vault with environment variable
{
  "VaultConfiguration": {
    "Provider": "securitas",
    "BaseUrl": "https://vault.production.example.com/api/v1",
    "AuthToken": "${VAULT_AUTH_TOKEN}",
    "ApiKey": ""
  }
}
```

**How Vault Integration Works:**

1. **Startup**: `VaultServiceFactory` reads `VaultConfiguration` and instantiates the appropriate vault service
2. **Detection**: `ConnectionStringBuilder` uses regex to detect `{vault:path}` placeholders in connection strings
3. **Resolution**: The vault service retrieves secrets from the configured vault endpoint
4. **Caching**: Retrieved secrets are cached in memory for 5 minutes to minimize API calls
5. **Connection**: Resolved connection strings (with actual passwords) are used for database connections

**Security Best Practices:**

✅ **DO:**
- Use environment variables for `AuthToken` and `ApiKey` (e.g., `"${VAULT_AUTH_TOKEN}"`)
- Use different vault endpoints per environment
- Restrict vault credentials to read-only permissions for specific secret paths
- Rotate vault credentials regularly according to security policies

❌ **DON'T:**
- Hardcode vault credentials in appsettings files
- Commit vault credentials to source control
- Use production vault credentials in non-production environments
- Share vault credentials across multiple services

### FileSystemProvider Section

Configure local or network file system storage for Parquet files:

```json
{
  "FileSystemProvider": {
    "BasePath": "C:\\temp\\DataLake",
    "MaxRetries": 3
  }
}
```

**Configuration Properties:**

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `BasePath` | Yes | - | Root directory path for storing Parquet files |
| `MaxRetries` | No | 3 | Number of retry attempts for failed file operations |

**Platform-Specific Path Formats:**

```json
// Windows
{
  "FileSystemProvider": {
    "BasePath": "C:\\temp\\DataLake",
    "MaxRetries": 3
  }
}

// Linux/Unix
{
  "FileSystemProvider": {
    "BasePath": "/var/datalake",
    "MaxRetries": 3
  }
}

// Network share (Windows)
{
  "FileSystemProvider": {
    "BasePath": "\\\\server\\share\\DataLake",
    "MaxRetries": 5
  }
}

// Mounted network drive (Linux)
{
  "FileSystemProvider": {
    "BasePath": "/mnt/datalake",
    "MaxRetries": 5
  }
}
```

**Environment-Specific Examples:**

```json
// Development - Local temp directory
{
  "FileSystemProvider": {
    "BasePath": "C:\\Temp\\DataLake",
    "MaxRetries": 3
  }
}

// Staging - Dedicated staging directory
{
  "FileSystemProvider": {
    "BasePath": "/var/datalake/staging",
    "MaxRetries": 5
  }
}

// Production - Production data lake mount
{
  "FileSystemProvider": {
    "BasePath": "/mnt/datalake/production",
    "MaxRetries": 5
  }
}
```

**Path Resolution:**

The `BasePath` is combined with the dataset's `Upload.Path` configuration to determine the final file location:

```
Final Path = {BasePath}/{Upload.Path}/{FileName}
Example: C:\temp\DataLake\trades\2024\12\29\trades_20241229_153045.parquet
```

**File Operation Retry Logic:**

When `MaxRetries` is configured:
- File write operations automatically retry on transient failures
- Exponential backoff between retry attempts
- Useful for network file systems with intermittent connectivity

**Permissions Requirements:**

Ensure the service account has:
- **Read/Write** permissions on the `BasePath` directory
- **Create subdirectories** permission for dataset-specific paths
- Sufficient **disk space** for Parquet file storage

### AzureBlob Section

Configure Azure Blob Storage as an upload destination:

```json
{
  "AzureBlob": {
    "ConnectionString": "",
    "DefaultContainer": "raw-data"
  }
}
```

**Configuration Properties:**

| Property | Required | Description | Example |
|----------|----------|-------------|---------|
| `ConnectionString` | Yes | Azure Storage account connection string | Can use vault placeholder |
| `DefaultContainer` | Yes | Default blob container name for uploads | `"raw-data"`, `"parquet-files"` |

**Connection String Formats:**

```json
// Using vault placeholder (recommended for production)
{
  "AzureBlob": {
    "ConnectionString": "{vault:azure/storage_connection_string}",
    "DefaultContainer": "raw-data"
  }
}

// Using standard connection string (development only)
{
  "AzureBlob": {
    "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net",
    "DefaultContainer": "dev-data"
  }
}

// Using Azurite local emulator (development)
{
  "AzureBlob": {
    "ConnectionString": "UseDevelopmentStorage=true",
    "DefaultContainer": "dev-data"
  }
}

// Using Managed Identity (production - requires code changes)
{
  "AzureBlob": {
    "ConnectionString": "BlobEndpoint=https://myaccount.blob.core.windows.net/",
    "DefaultContainer": "raw-data"
  }
}
```

**Environment-Specific Examples:**

```json
// Development - Azurite local emulator
{
  "AzureBlob": {
    "ConnectionString": "UseDevelopmentStorage=true",
    "DefaultContainer": "dev-data"
  }
}

// Staging - Staging storage account with vault
{
  "AzureBlob": {
    "ConnectionString": "{vault:staging/azure/storage_connection}",
    "DefaultContainer": "staging-raw-data"
  }
}

// Production - Production storage account with vault
{
  "AzureBlob": {
    "ConnectionString": "{vault:production/azure/storage_connection}",
    "DefaultContainer": "raw-data"
  }
}
```

**Container Naming Conventions:**

Azure Blob container names must follow these rules:
- Lowercase letters, numbers, and hyphens only
- Must start with a letter or number
- Length: 3-63 characters
- No consecutive hyphens

**Valid Examples:**
- ✅ `raw-data`
- ✅ `parquet-files`
- ✅ `trades-data-2024`

**Invalid Examples:**
- ❌ `Raw-Data` (uppercase)
- ❌ `-raw-data` (starts with hyphen)
- ❌ `raw--data` (consecutive hyphens)

**Using with Dataset Configuration:**

The `DefaultContainer` can be overridden per dataset:

```json
// Dataset configuration
{
  "upload": {
    "provider": "AzureBlob",
    "container": "trades-data",  // Overrides DefaultContainer
    "path": "trades/{year}/{month}/{day}",
    "fileNamePattern": "trades_{timestamp}.parquet"
  }
}
```

If `container` is not specified in the dataset configuration, `DefaultContainer` from `AzureBlob` section is used.

**Blob Path Structure:**

Final blob path is constructed as:

```
{Container}/{Upload.Path}/{FileName}
Example: raw-data/trades/2024/12/29/trades_20241229_153045.parquet
```

**Security Best Practices:**

✅ **DO:**
- Use vault placeholders for connection strings in non-development environments
- Use separate storage accounts for different environments
- Enable Azure Storage firewall rules to restrict access
- Use Shared Access Signatures (SAS) with minimal permissions when possible
- Rotate storage account keys regularly
- Enable blob versioning for audit trails

❌ **DON'T:**
- Commit connection strings with account keys to source control
- Use production storage accounts in development environments
- Grant excessive permissions (prefer read/write to specific containers only)
- Disable HTTPS (always use secure connections)

**Troubleshooting:**

**Error: "Container not found"**
```log
[ERR] Azure.RequestFailedException: The specified container does not exist
```
**Solution:** 
- Verify the container name matches exactly (case-sensitive in connection string, case-insensitive in Azure)
- Check if container exists in the storage account
- Ensure the connection string has permissions to access the container

---

**Error: "Authentication failed"**
```log
[ERR] Azure.RequestFailedException: Server failed to authenticate the request
```
**Solution:**
- Verify the connection string is correct and not expired
- Check if storage account key has been rotated
- Ensure vault placeholder is resolving correctly (if used)
- Verify firewall rules allow access from the service's IP address

---

**Error: "Blob name is invalid"**
```log
[ERR] Azure.RequestFailedException: The specified blob name contains invalid characters
```
**Solution:**
- Check `upload.path` and `fileNamePattern` in dataset configuration
- Ensure no special characters except `/`, `-`, `_`, `.` in blob paths
- Verify date/time placeholders are resolving correctly

### Datasets Configuration

The service supports hot-reloading of dataset configurations. Configure in `appsettings.json`:

```json
{
  "Datasets": {
    "ConfigurationPath": "./Datasets",     // Path to dataset JSON files
    "EnableHotReload": true,                // Enable hot reload
    "ReloadIntervalSeconds": 300            // Check for changes every 5 minutes
  }
}
```

When `EnableHotReload` is true, the service will automatically detect changes to dataset configuration files and reschedule jobs without requiring a restart.

### Dataset Configuration Schema

Each dataset is defined in a JSON file in the `Datasets/` directory:

```json
{
  "datasetId": "unique-identifier",           // Unique ID for the dataset
  "name": "Display Name",                     // Human-readable name
  "description": "Description",               // Optional description
  "enabled": true,                            // Enable/disable this dataset
  "cronExpression": "0 0 2 * * ?",           // Quartz cron expression

  "source": {
    "type": "SqlServer",                      // SqlServer | Oracle
    "connectionStringKey": "ConnectionName",  // Key from appsettings.json
    "extractionType": "StoredProcedure",      // StoredProcedure | Package | Query
    "procedureName": "dbo.sp_GetData",        // Stored procedure name
    "packageName": "PKG_DATA",                // Oracle package (if type=Package)
    "parameters": {                           // Input parameters
      "ParamName": "ParamValue"
    },
    "commandTimeout": 300                     // Timeout in seconds
  },

  "transformations": [                        // Optional transformation steps
    {
      "type": "DataCleansing",                 // Step class name (without "Step" suffix)
      "enabled": true,                          // Enable/disable this step
      "order": 1,                               // Execution order
      "config": {                               // Step-specific configuration
        "trimWhitespace": true,
        "removeEmptyStrings": false
      }
    },
    {
      "type": "DataValidation",
      "enabled": true,
      "order": 2,
      "config": {
        "requiredColumns": ["EMPLOYEE_ID", "EMAIL"],
        "validateEmail": true
      }
    }
  ],

  "parquet": {
    "fileNamePattern": "data_{date:yyyyMMdd}_{time:HHmmss}.parquet",
    "compressionCodec": "Snappy",             // Snappy | Gzip | None
    "rowGroupSize": 10000,                    // Rows per row group
    "enableStatistics": true                  // Enable Parquet statistics
  },

  "upload": {
    "provider": "FileSystem",                 // FileSystem | AzureBlob | AwsS3 | Axway
    "fileSystemConfig": {
      "basePath": "",                         // Override global BasePath
      "relativePath": "data/{year}/{month}/"  // Supports placeholders
    },
    "azureBlobConfig": {
      "containerName": "data",
      "blobPath": "raw/{year}/{month}/"
    },
    "overwriteExisting": false,
    "enableRetry": true,
    "maxRetries": 3
  },

  "notifications": {
    "onSuccess": false,
    "onFailure": true,
    "channels": ["email"]                     // Future: email, webhook, etc.
  },

  "metadata": {
    "owner": "Team Name",
    "contact": "team@company.com",
    "tags": ["sales", "daily"]
  }
}
```

### Oracle REF CURSOR Support

For Oracle data sources, the service automatically adds a REF CURSOR output parameter named `p_cursor`:

```sql
CREATE OR REPLACE PACKAGE PKG_SALES AS
    PROCEDURE GET_DAILY_SALES(
        p_start_date IN DATE,
        p_end_date IN DATE,
        p_cursor OUT SYS_REFCURSOR  -- Must be named 'p_cursor'
    );
END PKG_SALES;
```

**Dataset configuration:**
```json
{
  "source": {
    "type": "Oracle",
    "connectionStringKey": "HROracleDB",
    "extractionType": "Package",
    "packageName": "PKG_SALES",
    "procedureName": "GET_DAILY_SALES",
    "parameters": {
      "p_start_date": "2024-01-01",
      "p_end_date": "2024-12-31"
    }
  }
}
```

### Cross-Platform Paths

The service automatically detects the operating system and uses appropriate path separators:

**Windows:**
```json
"FileSystemProvider": {
  "BasePath": "C:\\DataLake\\Output"
}
```

**Linux:**
```json
"FileSystemProvider": {
  "BasePath": "/var/datalake/output"
}
```

### Quartz Cron Expression Examples

Quartz uses a 7-field cron format: `[second] [minute] [hour] [day-of-month] [month] [day-of-week] [year]`

| Expression | Description |
|-----------|-------------|
| `0 0 * * * ?` | Every hour at the top of the hour |
| `0 0 2 * * ?` | Every day at 2:00 AM |
| `0 */15 * * * ?` | Every 15 minutes |
| `0 0 6 ? * MON-FRI` | Weekdays at 6:00 AM |
| `0 0 0 1 * ?` | First day of every month at midnight |
| `0 0 12 ? * WED` | Every Wednesday at noon |
| `0 0 */4 * * ?` | Every 4 hours |

**Note:** The `?` character is used in day-of-month or day-of-week when you don't want to specify that field.

### Logging Configuration

Serilog is configured in `appsettings.json`:

```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "Microsoft": "Warning",
        "System": "Warning",
        "Quartz": "Information"
      }
    },
    "WriteTo": [
      {
        "Name": "Console"
      },
      {
        "Name": "File",
        "Args": {
          "path": "logs/datalake-.log",
          "rollingInterval": "Day",
          "retainedFileCountLimit": 30
        }
      }
    ]
  }
}
```

## Project Structure

```
DataLakeIngestionService/
├── src/
│   ├── Core/                                    # Domain Layer
│   │   ├── Enums/
│   │   │   └── Enums.cs                        # DataSourceType, ExtractionType, etc.
│   │   ├── Exceptions/
│   │   │   └── Exceptions.cs                   # Custom exceptions
│   │   ├── Handlers/
│   │   │   ├── ExtractionHandler.cs            # Step 1: Data extraction
│   │   │   ├── TransformationHandler.cs        # Step 2: Data transformation
│   │   │   ├── ParquetGenerationHandler.cs     # Step 3: Parquet generation
│   │   │   └── UploadHandler.cs                # Step 4: Upload
│   │   ├── Interfaces/                         # Service contracts
│   │   ├── Models/
│   │   │   └── DatasetConfiguration.cs         # Dataset model
│   │   └── Pipeline/
│   │       ├── BasePipelineHandler.cs          # Base handler
│   │       ├── DataPipeline.cs                 # Orchestrator
│   │       └── PipelineContext.cs              # Execution context
│   │
│   ├── Infrastructure/                          # Infrastructure Layer
│   │   ├── DataExtraction/
│   │   │   ├── SqlServerDataSource.cs          # Dapper SQL Server
│   │   │   ├── OracleDataSource.cs             # Oracle REF CURSOR
│   │   │   └── OracleDynamicParameters.cs      # Custom parameters
│   │   ├── Transformation/
│   │   │   ├── TransformationStepFactory.cs    # Auto-discovery factory
│   │   │   ├── TransformationEngine.cs         # Orchestrator
│   │   │   └── Common/
│   │   │       ├── DataCleansingStep.cs        # Trim whitespace
│   │   │       └── DataValidationStep.cs       # Validate columns
│   │   ├── Parquet/
│   │   │   └── ParquetWriterService.cs         # Parquet.Net writer
│   │   ├── Services/
│   │   │   ├── ConnectionStringBuilder.cs      # Connection string utilities
│   │   │   └── DatasetConfigurationService.cs  # Config management
│   │   ├── Upload/
│   │   │   ├── Providers/
│   │   │   │   ├── FileSystemUploadProvider.cs # Local/network upload
│   │   │   │   └── AzureBlobStorageProvider.cs # Azure Blob upload
│   │   │   └── UploadProviderFactory.cs
│   │   └── Vault/
│   │       ├── EvaVaultService.cs              # EVA vault integration
│   │       └── SecuritasVaultService.cs        # Securitas vault integration
│   │
│   └── Worker/                                  # Application Layer
│       ├── Extensions/
│       │   └── ServiceCollectionExtensions.cs  # DI registration
│       ├── Jobs/
│       │   └── DataIngestionJob.cs             # Quartz IJob
│       ├── Services/
│       │   └── JobSchedulingService.cs         # Job scheduler
│       ├── Datasets/                           # Dataset configs
│       ├── Program.cs                          # Entry point
│       └── appsettings.json                    # Configuration
│
├── tests/
│   ├── Core.Tests/
│   └── Infrastructure.Tests/
│
└── scripts/
    ├── install-service.ps1                     # Windows Service
    ├── install-daemon.sh                       # Linux systemd
    └── uninstall-service.ps1                   # Uninstall
```

## Core Components

### Pipeline Architecture

The service uses the **Chain of Responsibility** pattern with four sequential handlers:

```
ExtractionHandler → TransformationHandler → ParquetGenerationHandler → UploadHandler
```

Each handler:
1. Executes its specific task
2. Updates the `PipelineContext`
3. Passes control to the next handler
4. Handles errors and logs results

### Vault Services

The service includes extensible vault service support for secure credential management:

**Available Implementations:**
- `EvaVaultService` - Integration with EVA vault system
- `SecuritasVaultService` - Integration with Securitas vault system

**Interface:**
```csharp
public interface IVaultService
{
    // Vault operations for secure credential retrieval
}
```

Vault services can be used to retrieve connection strings and credentials securely instead of storing them in configuration files.

### Data Extraction (Dapper)

**SQL Server:**
```csharp
using var connection = new SqlConnection(connectionString);
var reader = await connection.ExecuteReaderAsync(
    "dbo.sp_GetData",
    dynamicParams,
    commandType: CommandType.StoredProcedure);
dataTable.Load(reader);
```

**Oracle with REF CURSOR:**
```csharp
var dynamicParams = new OracleDynamicParameters();
dynamicParams.Add("p_cursor", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

await connection.ExecuteAsync("PKG.PROC", dynamicParams, commandType: CommandType.StoredProcedure);

var refCursorParam = dynamicParams.GetOracleParameter("p_cursor");
if (refCursorParam.Value is Oracle.ManagedDataAccess.Types.OracleRefCursor refCursor)
{
    using var reader = refCursor.GetDataReader();
    dataTable.Load(reader);
}
```

### Parquet Generation

Automatic schema inference and Snappy compression:

```csharp
var fields = data.Columns.Cast<DataColumn>()
    .Select(col => new DataField(col.ColumnName, GetParquetType(col.DataType)))
    .ToArray();

using var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), outputStream);
writer.CompressionMethod = CompressionMethod.Snappy;
```

### Job Scheduling

Each dataset runs as an independent Quartz job:

```csharp
[DisallowConcurrentExecution]
public class DataIngestionJob : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        var datasetId = context.JobDetail.JobDataMap.GetString("DatasetId");
        // Load config, execute pipeline
    }
}
```

### Transformation Step Factory

The service uses a **factory pattern with auto-discovery** for transformation steps:

**How It Works:**

1. **Startup**: `TransformationStepFactory` scans assemblies at startup using reflection
2. **Discovery**: Finds all classes implementing `ITransformationStep`
3. **Registration**: Caches types in a dictionary (class name → Type)
4. **Execution**: Creates instances on-demand with DI-resolved dependencies

**Creating a New Transformation Step:**

```csharp
using System.Data;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Transformation.Common;

public class DateFormatStep : ITransformationStep
{
    private readonly ILogger<DateFormatStep> _logger;
    private readonly Dictionary<string, object> _config;

    // Constructor: ILogger resolved from DI, config passed by factory
    public DateFormatStep(
        ILogger<DateFormatStep> logger,
        Dictionary<string, object>? config = null)
    {
        _logger = logger;
        _config = config ?? new Dictionary<string, object>();
    }

    public string Name => "DateFormat";

    public Task<DataTable> TransformAsync(DataTable data, CancellationToken cancellationToken)
    {
        var columns = GetConfigValue<string[]>("columns", Array.Empty<string>());
        var outputFormat = GetConfigValue("outputFormat", "yyyy-MM-dd");
        
        _logger.LogInformation("Formatting date columns: {Columns}", string.Join(", ", columns));

        foreach (DataRow row in data.Rows)
        {
            foreach (var columnName in columns)
            {
                if (!data.Columns.Contains(columnName) || row[columnName] == DBNull.Value)
                    continue;

                if (DateTime.TryParse(row[columnName].ToString(), out var dateValue))
                {
                    row[columnName] = dateValue.ToString(outputFormat);
                }
            }
        }

        return Task.FromResult(data);
    }

    private T GetConfigValue<T>(string key, T defaultValue)
    {
        if (!_config.TryGetValue(key, out var value))
            return defaultValue;

        try
        {
            if (value is T typedValue)
                return typedValue;
                
            if (value is System.Text.Json.JsonElement jsonElement)
            {
                return System.Text.Json.JsonSerializer.Deserialize<T>(jsonElement.GetRawText())
                    ?? defaultValue;
            }
                
            return (T)Convert.ChangeType(value, typeof(T));
        }
        catch
        {
            return defaultValue;
        }
    }
}
```

**Using in Dataset Configuration:**

```json
{
  "datasetId": "sales-data",
  "transformations": [
    {
      "type": "DataCleansing",
      "enabled": true,
      "order": 1,
      "config": {
        "trimWhitespace": true,
        "removeEmptyStrings": false
      }
    },
    {
      "type": "DateFormat",
      "enabled": true,
      "order": 2,
      "config": {
        "columns": ["OrderDate", "ShipDate", "DeliveryDate"],
        "outputFormat": "yyyy-MM-dd HH:mm:ss"
      }
    },
    {
      "type": "DataValidation",
      "enabled": true,
      "order": 3,
      "config": {
        "requiredColumns": ["CustomerId", "OrderId"],
        "validateEmail": true
      }
    }
  ]
}
```

**Key Features:**

- ✅ **Zero Configuration**: No factory modification needed for new steps
- ✅ **Convention-Based**: Class name `DateFormatStep` → JSON type `"DateFormat"`
- ✅ **DI Integration**: Constructor dependencies auto-resolved
- ✅ **Per-Dataset Config**: Each step instance gets unique configuration
- ✅ **Explicit Ordering**: `order` property controls execution sequence
- ✅ **Hot Reload**: Adding new transformation classes is detected on service restart

**Available Transformation Steps:**

| Step | Type | Description | Config Options |
|------|------|-------------|----------------|
| **DataCleansingStep** | `DataCleansing` | Trims whitespace, removes empty strings | `trimWhitespace`, `removeEmptyStrings` |
| **DataValidationStep** | `DataValidation` | Validates required columns, email formats | `requiredColumns`, `validateEmail` |

**Extending with Custom Steps:**

1. Create a class implementing `ITransformationStep`
2. Place it in any project (Core or Infrastructure)
3. Build the solution
4. Reference it in dataset JSON using the class name (minus "Step" suffix)
5. No code changes to factory or DI registration required!

## Deployment

### Windows Service

```powershell
# Run as Administrator
cd scripts
.\install-service.ps1

# Manage service
sc.exe start DataLakeIngestionService
sc.exe stop DataLakeIngestionService
sc.exe query DataLakeIngestionService
```

### Linux systemd

```bash
# Run with sudo
cd scripts
chmod +x install-daemon.sh
sudo ./install-daemon.sh

# Manage service
sudo systemctl start datalake-ingestion
sudo systemctl stop datalake-ingestion
sudo systemctl status datalake-ingestion
sudo journalctl -u datalake-ingestion -f
```

## Monitoring & Logs

**Windows:**
- `[WorkingDirectory]/logs/datalake-YYYYMMDD.log`

**Linux:**
- `sudo journalctl -u datalake-ingestion`
- `/opt/datalake-ingestion/logs/datalake-YYYYMMDD.log`

**Sample Log:**
```
[INF] Starting Data Lake Ingestion Service
[INF] Loaded 2 dataset configurations
[INF] Scheduled job for dataset: Trades-daily-sqlserver with cron: 0 */2 * * * ?
[INF] Starting ingestion for dataset: Trades-daily-sqlserver
[INF] Executing SQL Server query: dbo.sp_GetDailyTrades
[INF] Retrieved 15432 rows from SQL Server
[INF] Successfully wrote 15432 rows to Parquet
[INF] Uploaded 3029 bytes to C:\temp\DataLake\Trades\Transcations\Trades_20251219_171200.parquet in 9ms
[INF] Successfully completed ingestion for dataset: Trades-daily-sqlserver, Duration: 0.7953737s
```

## Development

### Adding a New Upload Provider

1. Implement `IUploadProvider`:
```csharp
public class AwsS3Provider : IUploadProvider
{
    public async Task<IUploadResult> UploadAsync(...)
    {
        // Implementation
    }
}
```

2. Register in `UploadProviderFactory`
3. Add configuration in `ServiceCollectionExtensions`

### Adding Transformation Steps

1. Implement `ITransformationStep` in Infrastructure/Transformation/Common or any subfolder:
```csharp
using System.Data;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Transformation.Common;

public class ColumnMappingStep : ITransformationStep
{
    private readonly ILogger<ColumnMappingStep> _logger;
    private readonly Dictionary<string, object> _config;

    public ColumnMappingStep(
        ILogger<ColumnMappingStep> logger,
        Dictionary<string, object>? config = null)
    {
        _logger = logger;
        _config = config ?? new Dictionary<string, object>();
    }

    public string Name => "ColumnMapping";

    public Task<DataTable> TransformAsync(DataTable data, CancellationToken ct)
    {
        // Read column mappings from config
        var mappings = GetConfigValue<Dictionary<string, string>>("mappings", new());
        
        // Rename columns based on mappings
        foreach (var mapping in mappings)
        {
            if (data.Columns.Contains(mapping.Key))
            {
                data.Columns[mapping.Key].ColumnName = mapping.Value;
            }
        }
        
        return Task.FromResult(data);
    }

    private T GetConfigValue<T>(string key, T defaultValue)
    {
        if (!_config.TryGetValue(key, out var value))
            return defaultValue;

        try
        {
            if (value is T typedValue)
                return typedValue;
            
            if (value is System.Text.Json.JsonElement jsonElement)
            {
                return System.Text.Json.JsonSerializer.Deserialize<T>(jsonElement.GetRawText())
                    ?? defaultValue;
            }
                
            return (T)Convert.ChangeType(value, typeof(T));
        }
        catch
        {
            return defaultValue;
        }
    }
}
```

2. Use in dataset JSON configuration:
```json
{
  "transformations": [
    {
      "type": "ColumnMapping",
      "enabled": true,
      "order": 1,
      "config": {
        "mappings": {
          "old_column_name": "new_column_name",
          "EMPLOYEE_ID": "EmployeeId",
          "DEPT_NAME": "DepartmentName"
        }
      }
    }
  ]
}
```

3. **No factory or DI changes needed!** The `TransformationStepFactory` auto-discovers new implementations at startup

## Roadmap

- [ ] AWS S3 upload provider
- [ ] Axway secure file transfer
- [ ] Additional transformation steps
- [ ] Persistent Quartz store (SQL Server)
- [ ] Email/webhook notifications
- [ ] Prometheus metrics
- [ ] Unit and integration tests
- [ ] Docker containerization
- [ ] Kubernetes manifests
- [ ] Incremental extraction (CDC)

## License

MIT

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## Support

- **Issues:** https://github.com/danielcheehong/DataLakeIngestionService/issues
- **Documentation:** This README and inline code comments


