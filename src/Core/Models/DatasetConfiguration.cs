using DataLakeIngestionService.Core.Enums;

namespace DataLakeIngestionService.Core.Models;

public class DatasetConfiguration
{
    public string DatasetId { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public bool Enabled { get; set; } = true;
    public string CronExpression { get; set; } = "0 0 * * * ?";
    
    public SourceConfiguration Source { get; set; } = new();
    public List<TransformationConfiguration> Transformations { get; set; } = new();
    public ParquetConfiguration Parquet { get; set; } = new();
    public UploadConfiguration Upload { get; set; } = new();
    public NotificationConfiguration Notifications { get; set; } = new();
    public MetadataConfiguration Metadata { get; set; } = new();
}

public class SourceConfiguration
{
    public DataSourceType Type { get; set; }
    public string ConnectionStringKey { get; set; } = string.Empty;
    public ExtractionType ExtractionType { get; set; }
    public string ProcedureName { get; set; } = string.Empty;
    public string PackageName { get; set; } = string.Empty;
    public Dictionary<string, object> Parameters { get; set; } = new();
    public bool UseRefCursor { get; set; }
    public string RefCursorName { get; set; } = "p_cursor";
    public int CommandTimeout { get; set; } = 300;
}

public class TransformationConfiguration
{
    public string Type { get; set; } = string.Empty;
    public bool Enabled { get; set; } = true;
    public int Order { get; set; }
    public Dictionary<string, object> Config { get; set; } = new();
}

public class ParquetConfiguration
{
    public string FileNamePattern { get; set; } = "data_{date:yyyyMMdd}_{time:HHmmss}.parquet";
    public CompressionCodec CompressionCodec { get; set; } = CompressionCodec.Snappy;
    public int RowGroupSize { get; set; } = 5000;
    public bool EnableStatistics { get; set; } = true;
}

public class UploadConfiguration
{
    public UploadProviderType Provider { get; set; }
    public FileSystemConfig? FileSystemConfig { get; set; }
    public AzureBlobConfig? AzureBlobConfig { get; set; }
    public bool OverwriteExisting { get; set; }
    public bool EnableRetry { get; set; } = true;
    public int MaxRetries { get; set; } = 3;
}

public class FileSystemConfig
{
    public string BasePath { get; set; } = string.Empty;
    public string RelativePath { get; set; } = string.Empty;
}

public class AzureBlobConfig
{
    public string ContainerName { get; set; } = string.Empty;
    public string BlobPath { get; set; } = string.Empty;
}

public class NotificationConfiguration
{
    public bool OnSuccess { get; set; }
    public bool OnFailure { get; set; } = true;
    public List<string> Channels { get; set; } = new();
}

public class MetadataConfiguration
{
    public string Owner { get; set; } = string.Empty;
    public string Contact { get; set; } = string.Empty;
    public List<string> Tags { get; set; } = new();
}
