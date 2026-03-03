using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Core.Models;
using DataLakeIngestionService.Worker.Jobs;
using DataLakeIngestionService.Worker.Models;
using Quartz;
using Quartz.Impl.Matchers;

namespace DataLakeIngestionService.Worker.Services;

/// <summary>
/// Implementation of job management service for Quartz scheduler.
/// </summary>
public class JobManagementService : IJobManagementService
{
    private const string JobGroup = "DataIngestion";

    private static readonly System.Text.Json.JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
    };

    private readonly ISchedulerFactory _schedulerFactory;
    private readonly IDatasetConfigurationService _configService;
    private readonly ILogger<JobManagementService> _logger;

    public JobManagementService(
        ISchedulerFactory schedulerFactory,
        IDatasetConfigurationService configService,
        ILogger<JobManagementService> logger)
    {
        _schedulerFactory = schedulerFactory;
        _configService = configService;
        _logger = logger;
    }

    public async Task<List<ScheduledJobDto>> GetAllJobsAsync(CancellationToken cancellationToken = default)
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var jobKeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobGroup), cancellationToken);
        
        var jobs = new List<ScheduledJobDto>();

        foreach (var jobKey in jobKeys)
        {
            var jobDetail = await scheduler.GetJobDetail(jobKey, cancellationToken);
            var triggers = await scheduler.GetTriggersOfJob(jobKey, cancellationToken);
            var trigger = triggers.FirstOrDefault();

            var triggerState = trigger != null 
                ? await scheduler.GetTriggerState(trigger.Key, cancellationToken) 
                : TriggerState.None;

            string? cronExpression = null;
            if (trigger is ICronTrigger cronTrigger)
            {
                cronExpression = cronTrigger.CronExpressionString;
            }

            jobs.Add(new ScheduledJobDto
            {
                JobName = jobKey.Name,
                GroupName = jobKey.Group,
                DatasetId = jobDetail?.JobDataMap.GetString("DatasetId") ?? jobKey.Name,
                CronExpression = cronExpression,
                NextFireTime = trigger?.GetNextFireTimeUtc(),
                PreviousFireTime = trigger?.GetPreviousFireTimeUtc(),
                State = triggerState.ToString()
            });
        }

        return jobs;
    }

    public async Task<SchedulerStatusDto> GetSchedulerStatusAsync(CancellationToken cancellationToken = default)
    {
        var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        var metadata = await scheduler.GetMetaData(cancellationToken);

        return new SchedulerStatusDto
        {
            SchedulerName = scheduler.SchedulerName,
            SchedulerId = scheduler.SchedulerInstanceId,
            IsStarted = scheduler.IsStarted,
            IsShutdown = scheduler.IsShutdown,
            InStandbyMode = scheduler.InStandbyMode,
            NumberOfJobsExecuted = metadata.NumberOfJobsExecuted
        };
    }

    public async Task<JobTriggerResultDto> TriggerJobAsync(string datasetId, CancellationToken cancellationToken = default)
    {
        try
        {
            var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
            var jobKey = new JobKey(datasetId, JobGroup);

            if (!await scheduler.CheckExists(jobKey, cancellationToken))
            {
                return new JobTriggerResultDto
                {
                    Success = false,
                    Message = $"Job for dataset '{datasetId}' not found.",
                    DatasetId = datasetId
                };
            }

            await scheduler.TriggerJob(jobKey, cancellationToken);

            _logger.LogInformation("Triggered job immediately for dataset: {DatasetId}", datasetId);

            return new JobTriggerResultDto
            {
                Success = true,
                Message = $"Job for dataset '{datasetId}' triggered successfully.",
                DatasetId = datasetId,
                TriggeredAt = DateTimeOffset.UtcNow
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to trigger job for dataset: {DatasetId}", datasetId);
            return new JobTriggerResultDto
            {
                Success = false,
                Message = $"Failed to trigger job: {ex.Message}",
                DatasetId = datasetId
            };
        }
    }

    public async Task<JobTriggerResultDto> AddAndTriggerJobAsync(
        DatasetConfiguration config,
        bool triggerImmediately = true,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);

            // Always a temporary run-once job — unique ID to avoid any conflicts
            var runOnceId = $"{config.DatasetId}-runonce-{Guid.NewGuid().ToString("N")[..8]}";
            var jobKey = new JobKey(runOnceId, JobGroup);

            // Serialize the full posted config into the job data map
            var configJson = System.Text.Json.JsonSerializer.Serialize(config, _jsonOptions);

            var job = JobBuilder.Create<DataIngestionJob>()
                .WithIdentity(jobKey)
                .UsingJobData("DatasetId", runOnceId)
                .UsingJobData("ConfigOverride", configJson)
                .UsingJobData("IsRunOnce", "true")
                .StoreDurably(true)
                .Build();

            // Fire-once trigger — no cron schedule
            var trigger = TriggerBuilder.Create()
                .WithIdentity($"{runOnceId}-trigger", JobGroup)
                .StartNow()
                .WithSimpleSchedule(x => x.WithRepeatCount(0))
                .Build();

            await scheduler.ScheduleJob(job, trigger, cancellationToken);
            _logger.LogInformation("Scheduled run-once job: {RunOnceId} for dataset: {DatasetId}", runOnceId, config.DatasetId);

            if (triggerImmediately)
            {
                await scheduler.TriggerJob(jobKey, cancellationToken);
                _logger.LogInformation("Triggered run-once job immediately: {RunOnceId}", runOnceId);
            }

            return new JobTriggerResultDto
            {
                Success = true,
                Message = $"Run-once job '{runOnceId}' triggered for dataset '{config.DatasetId}'.",
                DatasetId = runOnceId,
                TriggeredAt = triggerImmediately ? DateTimeOffset.UtcNow : null
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to add/trigger run-once job for dataset: {DatasetId}", config.DatasetId);
            return new JobTriggerResultDto
            {
                Success = false,
                Message = $"Failed to add/trigger job: {ex.Message}",
                DatasetId = config.DatasetId
            };
        }
    }

    public async Task<JobRemovalResultDto> RemoveJobAsync(string datasetId, CancellationToken cancellationToken = default)
    {
        try
        {
            var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
            var jobKey = new JobKey(datasetId, JobGroup);

            if (!await scheduler.CheckExists(jobKey, cancellationToken))
            {
                return new JobRemovalResultDto
                {
                    Success = false,
                    Message = $"Job for dataset '{datasetId}' not found.",
                    DatasetId = datasetId
                };
            }

            var deleted = await scheduler.DeleteJob(jobKey, cancellationToken);

            _logger.LogInformation("Removed job for dataset: {DatasetId}", datasetId);

            return new JobRemovalResultDto
            {
                Success = deleted,
                Message = deleted
                    ? $"Job for dataset '{datasetId}' removed successfully."
                    : $"Failed to remove job for dataset '{datasetId}'.",
                DatasetId = datasetId
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to remove job for dataset: {DatasetId}", datasetId);
            return new JobRemovalResultDto
            {
                Success = false,
                Message = $"Failed to remove job: {ex.Message}",
                DatasetId = datasetId
            };
        }
    }

    public async Task<SchedulerOperationResultDto> PauseAllJobsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
            await scheduler.PauseAll(cancellationToken);

            _logger.LogInformation("Paused all scheduled jobs");

            return new SchedulerOperationResultDto
            {
                Success = true,
                Message = "All jobs paused successfully.",
                Operation = "PauseAll"
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to pause all jobs");
            return new SchedulerOperationResultDto
            {
                Success = false,
                Message = $"Failed to pause all jobs: {ex.Message}",
                Operation = "PauseAll"
            };
        }
    }

    public async Task<SchedulerOperationResultDto> ResumeAllJobsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
            await scheduler.ResumeAll(cancellationToken);

            _logger.LogInformation("Resumed all scheduled jobs");

            return new SchedulerOperationResultDto
            {
                Success = true,
                Message = "All jobs resumed successfully.",
                Operation = "ResumeAll"
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to resume all jobs");
            return new SchedulerOperationResultDto
            {
                Success = false,
                Message = $"Failed to resume all jobs: {ex.Message}",
                Operation = "ResumeAll"
            };
        }
    }

    public async Task<RescheduleResultDto> RescheduleAllJobsAsync(CancellationToken cancellationToken = default)
    {
        var result = new RescheduleResultDto();
        
        try
        {
            var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
            
            // Clear all existing jobs in the group
            var existingJobs = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobGroup), cancellationToken);
            foreach (var jobKey in existingJobs)
            {
                await scheduler.DeleteJob(jobKey, cancellationToken);
            }

            _logger.LogInformation("Cleared {Count} existing jobs", existingJobs.Count);

            // Reload configurations and reschedule
            var datasets = await _configService.GetDatasetsAsync();

            foreach (var dataset in datasets.Where(d => d.Enabled))
            {
                try
                {
                    var jobKey = new JobKey(dataset.DatasetId, JobGroup);

                    var job = JobBuilder.Create<DataIngestionJob>()
                        .WithIdentity(jobKey)
                        .UsingJobData("DatasetId", dataset.DatasetId)
                        .Build();

                    var trigger = TriggerBuilder.Create()
                        .WithIdentity($"{dataset.DatasetId}-trigger", JobGroup)
                        .WithCronSchedule(dataset.CronExpression)
                        .Build();

                    await scheduler.ScheduleJob(job, trigger, cancellationToken);
                    result.ScheduledDatasets.Add(dataset.DatasetId);

                    _logger.LogInformation(
                        "Rescheduled job for dataset: {DatasetId} with cron: {CronExpression}",
                        dataset.DatasetId, dataset.CronExpression);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to reschedule job for dataset: {DatasetId}", dataset.DatasetId);
                    result.FailedDatasets.Add(dataset.DatasetId);
                }
            }

            result.Success = result.FailedDatasets.Count == 0;
            result.JobsScheduled = result.ScheduledDatasets.Count;
            result.Message = result.Success
                ? $"Successfully rescheduled {result.JobsScheduled} jobs."
                : $"Rescheduled {result.JobsScheduled} jobs with {result.FailedDatasets.Count} failures.";

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to reschedule all jobs");
            return new RescheduleResultDto
            {
                Success = false,
                Message = $"Failed to reschedule jobs: {ex.Message}"
            };
        }
    }

    public async Task<DatasetConfigUpdateResultDto> UpdateDatasetConfigAsync(
        string datasetId,
        string? cronExpression,
        Dictionary<string, string>? parameterUpdates,
        string? uploadProvider,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(cronExpression)
                && (parameterUpdates == null || parameterUpdates.Count == 0)
                && string.IsNullOrWhiteSpace(uploadProvider))
            {
                return new DatasetConfigUpdateResultDto
                {
                    Success = false,
                    Message = "At least one of cronExpression, parameterUpdates, or uploadProvider must be supplied.",
                    DatasetId = datasetId
                };
            }

            var updatedConfig = await _configService.UpdateDatasetFileAsync(
                datasetId,
                cronExpression?.Trim(),
                parameterUpdates,
                uploadProvider?.Trim(),
                cancellationToken);

            // Live-reschedule Quartz trigger if cron expression changed
            if (!string.IsNullOrWhiteSpace(cronExpression))
            {
                try
                {
                    var scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
                    var triggerKey = new TriggerKey($"{datasetId}-trigger", JobGroup);
                    var existingTrigger = await scheduler.GetTrigger(triggerKey, cancellationToken);

                    if (existingTrigger != null)
                    {
                        var newTrigger = TriggerBuilder.Create()
                            .WithIdentity(triggerKey)
                            .ForJob(new JobKey(datasetId, JobGroup))
                            .WithCronSchedule(cronExpression)
                            .Build();

                        await scheduler.RescheduleJob(triggerKey, newTrigger, cancellationToken);
                        _logger.LogInformation(
                            "Live-rescheduled job '{DatasetId}' with cron '{CronExpression}'",
                            datasetId, cronExpression);
                    }
                }
                catch (Exception ex)
                {
                    // File and cache already updated — log but do not fail the response
                    _logger.LogWarning(ex,
                        "Config updated but live Quartz reschedule failed for dataset '{DatasetId}'", datasetId);
                }
            }

            return new DatasetConfigUpdateResultDto
            {
                Success = true,
                Message = $"Dataset configuration for '{datasetId}' updated successfully.",
                DatasetId = datasetId,
                UpdatedConfig = updatedConfig
            };
        }
        catch (KeyNotFoundException)
        {
            return new DatasetConfigUpdateResultDto
            {
                Success = false,
                Message = $"Dataset '{datasetId}' not found.",
                DatasetId = datasetId
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update dataset config for '{DatasetId}'", datasetId);
            return new DatasetConfigUpdateResultDto
            {
                Success = false,
                Message = $"Failed to update dataset config: {ex.Message}",
                DatasetId = datasetId
            };
        }
    }
}
