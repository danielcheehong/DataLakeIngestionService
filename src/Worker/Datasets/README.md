# Dataset Configuration Files

This directory contains JSON configuration files for each dataset that will be ingested by the Data Lake Ingestion Service.

## File Naming Convention

All dataset configuration files must follow the pattern: `dataset-*.json`

## Sample Configuration

See `dataset-sales-sqlserver.json` for a complete example.

## Configuration Properties

- **datasetId**: Unique identifier for the dataset
- **name**: Human-readable name
- **enabled**: Whether the dataset should be processed
- **cronExpression**: Quartz cron expression for scheduling
- **source**: Database source configuration
- **transformations**: List of data transformations to apply
- **parquet**: Parquet file generation settings
- **upload**: Upload destination configuration
- **notifications**: Notification settings
- **metadata**: Additional metadata for documentation

## Cross-Platform Paths

### Windows
```json
"basePath": "C:\\DataLake\\Output"
"basePath": "\\\\fileserver\\share\\data"
```

### Linux
```json
"basePath": "/var/datalake/output"
"basePath": "/mnt/fileserver/data"
```
