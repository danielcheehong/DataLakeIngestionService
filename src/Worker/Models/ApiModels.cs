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
