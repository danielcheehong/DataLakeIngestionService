using DataLakeIngestionService.Core.Enums;

namespace DataLakeIngestionService.Core.Models;

public class DatasetConfiguration
{
    public string DatasetId { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public bool Enabled { get; set; } = true;
    public string CronExpression { get; set; } = "0 0 * * * ?";
    
    /// <summary>
    /// Single source configuration. Used for backward compatibility.
    /// If Sources is populated, this property is ignored.
    /// </summary>
    public SourceConfiguration Source { get; set; } = new();
    
    /// <summary>
    /// Multiple source configurations for datasets requiring data from multiple datasources.
    /// When populated, takes precedence over the single Source property.
    /// Each source must have a unique SourceId.
    /// </summary>
    public List<SourceConfiguration>? Sources { get; set; }
    
    public List<TransformationConfiguration> Transformations { get; set; } = new();
    public ParquetConfiguration Parquet { get; set; } = new();
    public UploadConfiguration Upload { get; set; } = new();
    public NotificationConfiguration Notifications { get; set; } = new();
    public MetadataConfiguration Metadata { get; set; } = new();
    
    /// <summary>
    /// Returns true if this dataset uses multiple sources.
    /// </summary>
    public bool HasMultipleSources => Sources != null && Sources.Count > 0;
}

public class SourceConfiguration
{
    /// <summary>
    /// Unique identifier for this source within the dataset.
    /// Required when using multiple sources. Used as the key in ExtractedDataSets.
    /// </summary>
    public string SourceId { get; set; } = string.Empty;
    
    public DataSourceType Type { get; set; }
    public string ConnectionStringKey { get; set; } = string.Empty;
    public ExtractionType ExtractionType { get; set; }
    public string ProcedureName { get; set; } = string.Empty;
    public string PackageName { get; set; } = string.Empty;
    /// <summary>
    /// SQL file name (relative to Datasets/SqlFiles folder) for Query extraction type.
    /// </summary>
    public string SqlFilePath { get; set; } = string.Empty;
    /// <summary>
    /// For DotNet source type: the registered provider name to invoke.
    /// This should match the ProviderName property of a registered IDotNetDataProvider implementation.
    /// </summary>
    public string ProviderName { get; set; } = string.Empty;
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
    
    /// <summary>
    /// List of environments where this transformation should execute.
    /// Empty or null list means execute in ALL environments.
    /// Valid values: "Development", "Staging", "Production", "DR"
    /// </summary>
    public List<string> Environments { get; set; } = new();
    
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
    public AxwayConfig? AxwayConfig { get; set; }
    public bool OverwriteExisting { get; set; }
    public bool EnableRetry { get; set; } = true;
    public int MaxRetries { get; set; } = 3;
    
    /// <summary>
    /// When true, keeps a local copy of Parquet and CTL files after uploading to remote destination.
    /// </summary>
    public bool KeepLocalCopy { get; set; } = false;
    
    /// <summary>
    /// Local directory path where copies of uploaded files will be stored when KeepLocalCopy is true.
    /// </summary>
    public string LocalCopyPath { get; set; } = string.Empty;
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

public class AxwayConfig
{
    public string DestinationPath { get; set; } = string.Empty;
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
