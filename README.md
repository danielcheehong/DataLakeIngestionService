# Data Lake Ingestion Service

A cross-platform .NET 8 background service for extracting data from SQL Server and Oracle databases, applying transformations, generating Parquet files, and uploading to various storage destinations.

## Features

- **Multi-Database Support**: Extract from SQL Server (stored procedures) and Oracle (packages with REF CURSORs)
- **Chain of Responsibility Pipeline**: Modular extraction → transformation → Parquet generation → upload pipeline
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
        │  - Gets connection string               │
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
  "ConnectionStrings": {
    "SalesSqlServer": "Server=localhost;Database=Sales;Integrated Security=true;TrustServerCertificate=true;",
    "HROracleDB": "Data Source=localhost:1521/ORCL;User Id=hr_service;Password=YourPassword;",
    "InventoryDB": "Server=localhost;Database=Warehouse;Integrated Security=true;TrustServerCertificate=true;"
  },
  
  "FileSystemProvider": {
    "BasePath": "C:\\DataLake\\Output",  // Windows
    // "BasePath": "/var/datalake/output",  // Linux
    "MaxRetries": 3
  },
  
  "AzureBlob": {
    "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;",
    "DefaultContainer": "raw-data"
  }
}
```

### 3. Create Dataset Configuration

Create a JSON file in `src/Worker/Datasets/` (e.g., `dataset-sales-sqlserver.json`):

```json
{
  "datasetId": "sales-daily-sqlserver",
  "name": "Daily Sales Transactions",
  "description": "Extract daily sales data from SQL Server",
  "enabled": true,
  "cronExpression": "0 0 2 * * ?",  // Every day at 2 AM

  "source": {
    "type": "SqlServer",
    "connectionStringKey": "SalesSqlServer",
    "extractionType": "StoredProcedure",
    "procedureName": "dbo.sp_GetDailySales",
    "parameters": {
      "StartDate": "2024-01-01",
      "EndDate": "2024-12-31"
    },
    "commandTimeout": 300
  },

  "transformations": [],

  "parquet": {
    "fileNamePattern": "sales_{date:yyyyMMdd}_{time:HHmmss}.parquet",
    "compressionCodec": "Snappy",
    "rowGroupSize": 10000,
    "enableStatistics": true
  },

  "upload": {
    "provider": "FileSystem",
    "fileSystemConfig": {
      "basePath": "",
      "relativePath": "sales/{year}/{month}/{day}/"
    },
    "overwriteExisting": false,
    "enableRetry": true,
    "maxRetries": 3
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
[INF] Loaded 1 dataset configurations
[INF] Scheduled job for dataset: sales-daily-sqlserver with cron: 0 0 2 * * ?
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
    "DataCleansing",
    "DataValidation"
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
│   │   │   └── TransformationEngine.cs         # Transformation steps
│   │   ├── Parquet/
│   │   │   └── ParquetWriterService.cs         # Parquet.Net writer
│   │   └── Upload/
│   │       ├── Providers/
│   │       │   ├── FileSystemUploadProvider.cs # Local/network upload
│   │       │   └── AzureBlobStorageProvider.cs # Azure Blob upload
│   │       └── UploadProviderFactory.cs
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
[INF] Loaded 3 dataset configurations
[INF] Scheduled job for dataset: sales-daily-sqlserver with cron: 0 0 2 * * ?
[INF] Starting ingestion for dataset: sales-daily-sqlserver
[INF] Executing SQL Server query: dbo.sp_GetDailySales
[INF] Retrieved 15432 rows from SQL Server
[INF] Successfully wrote 15432 rows to Parquet
[INF] File uploaded successfully: C:\DataLake\Output\sales\2024\12\15\sales_20241215_020000.parquet
[INF] Successfully completed ingestion, Duration: 12.34s
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

1. Implement `ITransformationStep`:
```csharp
public class DataTypeConversionStep : ITransformationStep
{
    public string Name => "DataTypeConversion";
    public Task<DataTable> TransformAsync(DataTable data, CancellationToken ct)
    {
        // Logic
    }
}
```

2. Reference in dataset configuration

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


