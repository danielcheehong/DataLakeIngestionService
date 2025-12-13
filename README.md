# DataLakeIngestionService

A .NET service designed to extract, transform, and load data from multiple database sources into a data lake storage system using the Parquet file format.

## Overview

DataLakeIngestionService is a robust ETL (Extract, Transform, Load) pipeline that facilitates the movement of data from enterprise databases (SQL Server and Oracle) into cloud or on-premises data lake storage. The service handles data extraction, applies configurable transformations, and efficiently stores data in columnar Parquet format for optimized analytics workloads.

## Features

- **Multi-Source Data Extraction**
  - SQL Server stored procedure execution
  - Oracle package invocation
  - Support for complex query patterns and parameterized procedures

- **Flexible Data Pipeline**
  - Configurable step-based transformation engine
  - Chain multiple transformation steps
  - Support for data validation, cleansing, and enrichment

- **Parquet File Generation**
  - Efficient columnar storage format
  - Optimized for analytics and query performance
  - Compression support for reduced storage costs

- **Multiple Upload Methods**
  - Axway integration for secure file transfer
  - Direct cloud storage uploads (Azure Blob Storage, AWS S3, etc.)
  - Extensible upload provider architecture

## Architecture

```
┌─────────────────┐
│  Data Sources   │
│  - SQL Server   │
│  - Oracle       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Extraction    │
│   - Stored Procs│
│   - Packages    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Data Pipeline   │
│ Transformations │
│  - Step 1       │
│  - Step 2       │
│  - Step N       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Parquet Writer  │
│ File Generation │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Upload Service  │
│  - Axway        │
│  - Cloud Storage│
└─────────────────┘
```

## Prerequisites

- .NET 6.0 or later
- SQL Server client libraries
- Oracle Data Access Components (ODAC)
- Access credentials for source databases
- Axway credentials (if using Axway for file transfer)
- Target storage credentials

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/DataLakeIngestionService.git
cd DataLakeIngestionService
```

2. Restore dependencies:
```bash
dotnet restore
```

3. Configure connection strings and settings (see Configuration section)

4. Build the solution:
```bash
dotnet build
```

## Configuration

Update `appsettings.json` with your environment-specific settings:

```json
{
  "ConnectionStrings": {
    "SqlServer": "Server=your-server;Database=your-db;...",
    "Oracle": "Data Source=your-oracle-instance;User Id=user;Password=pwd;"
  },
  "Pipeline": {
    "TransformationSteps": [
      {
        "Name": "DataCleansing",
        "Enabled": true
      },
      {
        "Name": "DataValidation",
        "Enabled": true
      }
    ]
  },
  "Storage": {
    "Provider": "Axway",
    "AxwayConfig": {
      "Endpoint": "https://axway-endpoint",
      "Username": "your-username",
      "Password": "your-password"
    },
    "TargetPath": "/datalake/ingestion/"
  },
  "Parquet": {
    "CompressionCodec": "Snappy",
    "RowGroupSize": 5000
  }
}
```

## Usage

### Running the Service

```bash
dotnet run --project DataLakeIngestionService
```

### Command Line Options

```bash
dotnet run -- --source SqlServer --procedure GetCustomerData --output /data/customers/
```

### Scheduled Execution

Configure the service to run on a schedule using:
- Windows Task Scheduler
- Cron jobs (Linux)
- Azure Functions Timer Trigger
- Kubernetes CronJob

## Data Pipeline Transformations

The service supports pluggable transformation steps:

- **Data Type Conversion**: Convert between different data types
- **Data Cleansing**: Remove invalid characters, trim whitespace
- **Data Validation**: Validate against business rules
- **Data Enrichment**: Add calculated fields or lookup values
- **Data Filtering**: Filter rows based on conditions
- **Column Mapping**: Rename or select specific columns

## Monitoring and Logging

The service includes comprehensive logging:
- Extraction metrics (rows extracted, duration)
- Transformation results
- File generation statistics
- Upload status and errors

Logs are written to:
- Console output
- Log files (configurable location)
- Application Insights (optional)

## Error Handling

- Automatic retry logic for transient failures
- Dead letter queue for failed extractions
- Detailed error logging with stack traces
- Alerting integration (email, Slack, etc.)

## Performance Considerations

- Batch processing for large datasets
- Parallel extraction from multiple sources
- Memory-efficient streaming operations
- Configurable chunk sizes for Parquet writing

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Specify your license here]

## Support

For issues and questions:
- Create an issue in the GitHub repository
- Contact the development team at [your-email]

## Roadmap

- [ ] Support for additional data sources (MySQL, PostgreSQL)
- [ ] Real-time streaming ingestion
- [ ] Delta Lake format support
- [ ] Advanced data quality checks
- [ ] Web-based monitoring dashboard
