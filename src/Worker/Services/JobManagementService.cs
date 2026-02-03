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
            var jobKey = new JobKey(config.DatasetId, JobGroup);

            // Delete existing job if present
            if (await scheduler.CheckExists(jobKey, cancellationToken))
            {
                await scheduler.DeleteJob(jobKey, cancellationToken);
                _logger.LogInformation("Deleted existing job for dataset: {DatasetId}", config.DatasetId);
            }

            // Create new job
            var job = JobBuilder.Create<DataIngestionJob>()
                .WithIdentity(jobKey)
                .UsingJobData("DatasetId", config.DatasetId)
                .StoreDurably(true)
                .Build();

            // Create trigger with cron schedule
            var trigger = TriggerBuilder.Create()
                .WithIdentity($"{config.DatasetId}-trigger", JobGroup)
                .WithCronSchedule(config.CronExpression)
                .Build();

            await scheduler.ScheduleJob(job, trigger, cancellationToken);

            _logger.LogInformation(
                "Scheduled job for dataset: {DatasetId} with cron: {CronExpression}",
                config.DatasetId, config.CronExpression);

            // Trigger immediately if requested
            if (triggerImmediately)
            {
                await scheduler.TriggerJob(jobKey, cancellationToken);
                _logger.LogInformation("Triggered job immediately for dataset: {DatasetId}", config.DatasetId);
            }

            return new JobTriggerResultDto
            {
                Success = true,
                Message = triggerImmediately
                    ? $"Job for dataset '{config.DatasetId}' scheduled and triggered immediately."
                    : $"Job for dataset '{config.DatasetId}' scheduled successfully.",
                DatasetId = config.DatasetId,
                TriggeredAt = triggerImmediately ? DateTimeOffset.UtcNow : null
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to add/trigger job for dataset: {DatasetId}", config.DatasetId);
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
}
