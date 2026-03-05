using System.Data;
using DataLakeIngestionService.Core.Enums;
using DataLakeIngestionService.Core.Security;

namespace DataLakeIngestionService.Core.Pipeline;

public interface IPipelineContext
{
    string JobId { get; }
    DateTime StartTime { get; }
    IDictionary<string, object> Metadata { get; }
    DataTable? ExtractedData { get; set; }
    
    /// <summary>
    /// Dictionary of extracted DataTables keyed by source identifier.
    /// Used for multi-source datasets where each source produces a separate DataTable.
    /// </summary>
    IDictionary<string, DataTable> ExtractedDataSets { get; }
    
    byte[]? ParquetData { get; set; }
    byte[]? CtlData { get; set; }
    string? CtlFileName { get; set; }
    string? UploadUri { get; set; }
    bool HasErrors { get; }
    List<PipelineError> Errors { get; }
    CancellationToken CancellationToken { get; }
}

public class PipelineContext : IPipelineContext
{
    public string JobId { get; set; } = Guid.NewGuid().ToString();
    public DateTime StartTime { get; set; } = DateTime.UtcNow;
    public IDictionary<string, object> Metadata { get; set; } = new Dictionary<string, object>();
    public DataTable? ExtractedData { get; set; }
    public IDictionary<string, DataTable> ExtractedDataSets { get; set; } = new Dictionary<string, DataTable>();
    public byte[]? ParquetData { get; set; }
    public byte[]? CtlData { get; set; }
    public string? CtlFileName { get; set; }
    public string? UploadUri { get; set; }
    public bool HasErrors => Errors.Any();
    public List<PipelineError> Errors { get; set; } = new();
    public CancellationToken CancellationToken { get; set; }
}

public class PipelineError
{
    public string Stage { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public Exception? Exception { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public ErrorSeverity Severity { get; set; }
}

public class PipelineResult
{
    public bool IsSuccess { get; set; }
    public string Message { get; set; } = string.Empty;
    public bool ShouldContinue { get; set; } = true;
    public Dictionary<string, object> StageMetrics { get; set; } = new();
}

public class PipelineExecutionResult
{
    public string JobId { get; set; } = string.Empty;
    public bool IsSuccess { get; set; }
    public string Message { get; set; } = string.Empty;
    public List<PipelineError> Errors { get; set; } = new();
    public TimeSpan TotalDuration { get; set; }
    public string? UploadUri { get; set; }
}

/// <summary>
/// Configuration for a single source extraction, passed via metadata for multi-source datasets.
/// Implements IDisposable so that the connection secret (char[] buffer) is zeroed as soon as
/// the pipeline has finished consuming it.
/// </summary>
public class SourceExtractionConfig : IDisposable
{
    private bool _disposed;

    public string SourceId { get; set; } = string.Empty;
    public string SourceType { get; set; } = string.Empty;
    public Core.Enums.ExtractionType ExtractionType { get; set; }

    /// <summary>
    /// Holds the resolved connection string in a zeroable char[] buffer.
    /// Null for DotNet (code-generator) sources that do not require a connection.
    /// </summary>
    public SecretValue? ConnectionSecret { get; set; }

    public string Query { get; set; } = string.Empty;
    public Dictionary<string, object> Parameters { get; set; } = new();

    public void Dispose()
    {
        if (_disposed) return;
        ConnectionSecret?.Dispose();
        _disposed = true;
    }
}
