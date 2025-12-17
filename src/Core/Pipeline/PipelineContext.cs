using System.Data;
using DataLakeIngestionService.Core.Enums;

namespace DataLakeIngestionService.Core.Pipeline;

public interface IPipelineContext
{
    string JobId { get; }
    DateTime StartTime { get; }
    IDictionary<string, object> Metadata { get; }
    DataTable? ExtractedData { get; set; }
    byte[]? ParquetData { get; set; }
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
    public byte[]? ParquetData { get; set; }
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
