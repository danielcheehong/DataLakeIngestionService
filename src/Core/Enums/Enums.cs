namespace DataLakeIngestionService.Core.Enums;

public enum DataSourceType
{
    SqlServer,
    Oracle
}

public enum ExtractionType
{
    StoredProcedure,
    Package,
    Query
}

public enum TransformationType
{
    DataCleansing,
    DataValidation,
    DataTypeConversion,
    DataFiltering,
    DataEnrichment,
    ColumnMapping,
    NullHandling,
    DateFormat
}

public enum UploadProviderType
{
    FileSystem,
    AzureBlob,
    AwsS3,
    Axway
}

public enum ErrorSeverity
{
    Warning,
    Error,
    Critical
}

public enum IngestionStatus
{
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled
}

public enum CompressionCodec
{
    None,
    Snappy,
    Gzip,
    Lz4,
    Brotli
}
