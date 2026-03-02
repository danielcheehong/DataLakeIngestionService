using DataLakeIngestionService.Core.Models;

namespace DataLakeIngestionService.Worker.Models;

/// <summary>
/// Represents a scheduled job in the system.
/// </summary>
public class ScheduledJobDto
{
    public string JobName { get; set; } = string.Empty;
    public string GroupName { get; set; } = string.Empty;
    public string DatasetId { get; set; } = string.Empty;
    public string? CronExpression { get; set; }
    public DateTimeOffset? NextFireTime { get; set; }
    public DateTimeOffset? PreviousFireTime { get; set; }
    public string State { get; set; } = string.Empty;
}

/// <summary>
/// Response for scheduler status operations.
/// </summary>
public class SchedulerStatusDto
{
    public string SchedulerName { get; set; } = string.Empty;
    public string SchedulerId { get; set; } = string.Empty;
    public bool IsStarted { get; set; }
    public bool IsShutdown { get; set; }
    public bool InStandbyMode { get; set; }
    public int NumberOfJobsExecuted { get; set; }
}

/// <summary>
/// Response for job trigger operations.
/// </summary>
public class JobTriggerResultDto
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public string? DatasetId { get; set; }
    public DateTimeOffset? TriggeredAt { get; set; }
}

/// <summary>
/// Response for job removal operations.
/// </summary>
public class JobRemovalResultDto
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public string? DatasetId { get; set; }
}

/// <summary>
/// Response for scheduler pause/resume operations.
/// </summary>
public class SchedulerOperationResultDto
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public string Operation { get; set; } = string.Empty;
}

/// <summary>
/// Response for reschedule all jobs operation.
/// </summary>
public class RescheduleResultDto
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public int JobsScheduled { get; set; }
    public List<string> ScheduledDatasets { get; set; } = new();
    public List<string> FailedDatasets { get; set; } = new();
}

/// <summary>
/// Request body for PATCH /api/datasets/{datasetId}/config.
/// All fields are optional; at least one must be supplied.
/// </summary>
public class DatasetConfigUpdateRequest
{
    /// <summary>Quartz cron expression (6-part, seconds-first, e.g. "0 0 6 * * ?"). Optional.</summary>
    public string? CronExpression { get; set; }

    /// <summary>
    /// Named parameter values to update inside source / sources[*].parameters.
    /// Key = exact parameter name as it appears in the JSON (e.g. "p_ref_date").
    /// Value = new value (e.g. "2025-12-01").
    /// Only parameters that already exist in the config are updated; no new keys are injected.
    /// </summary>
    public Dictionary<string, string>? ParameterUpdates { get; set; }

    /// <summary>Upload provider name ("FileSystem", "AzureBlob", "AwsS3", "Axway"). Optional.</summary>
    public string? UploadProvider { get; set; }
}

/// <summary>
/// Response for PATCH /api/datasets/{datasetId}/config.
/// </summary>
public class DatasetConfigUpdateResultDto
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public string? DatasetId { get; set; }
    /// <summary>The full updated dataset configuration. Null on failure.</summary>
    public DatasetConfiguration? UpdatedConfig { get; set; }
}
