using DataLakeIngestionService.Core.Models;
using DataLakeIngestionService.Worker.Models;

namespace DataLakeIngestionService.Worker.Services;

/// <summary>
/// Service for managing Quartz scheduler jobs via API.
/// </summary>
public interface IJobManagementService
{
    /// <summary>
    /// Gets all scheduled jobs.
    /// </summary>
    Task<List<ScheduledJobDto>> GetAllJobsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current scheduler status.
    /// </summary>
    Task<SchedulerStatusDto> GetSchedulerStatusAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Triggers an existing job immediately by dataset ID.
    /// </summary>
    Task<JobTriggerResultDto> TriggerJobAsync(string datasetId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a new job from configuration and optionally triggers it immediately.
    /// </summary>
    Task<JobTriggerResultDto> AddAndTriggerJobAsync(
        DatasetConfiguration config, 
        bool triggerImmediately = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a scheduled job by dataset ID.
    /// </summary>
    Task<JobRemovalResultDto> RemoveJobAsync(string datasetId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Pauses all jobs in the scheduler.
    /// </summary>
    Task<SchedulerOperationResultDto> PauseAllJobsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Resumes all paused jobs in the scheduler.
    /// </summary>
    Task<SchedulerOperationResultDto> ResumeAllJobsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reschedules all jobs from the dataset configuration files.
    /// </summary>
    Task<RescheduleResultDto> RescheduleAllJobsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Surgically updates the dataset JSON config file (cronExpression, named parameter values,
    /// upload provider), live-reschedules the Quartz trigger if the cron changed, and returns
    /// the full updated configuration.
    /// </summary>
    Task<DatasetConfigUpdateResultDto> UpdateDatasetConfigAsync(
        string datasetId,
        string? cronExpression,
        Dictionary<string, string>? parameterUpdates,
        string? uploadProvider,
        CancellationToken cancellationToken = default);
}
