using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Worker.Jobs;
using Quartz;

namespace DataLakeIngestionService.Worker.Services;

public class JobSchedulingService : IHostedService
{
    private readonly ISchedulerFactory _schedulerFactory;
    private readonly IDatasetConfigurationService _configService;
    private readonly ILogger<JobSchedulingService> _logger;
    private IScheduler? _scheduler;

    public JobSchedulingService(
        ISchedulerFactory schedulerFactory,
        IDatasetConfigurationService configService,
        ILogger<JobSchedulingService> logger)
    {
        _schedulerFactory = schedulerFactory;
        _configService = configService;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting Job Scheduling Service");

        _scheduler = await _schedulerFactory.GetScheduler(cancellationToken);
        await _scheduler.Start(cancellationToken);

        // Load all dataset configurations and schedule jobs
        var datasets = await _configService.GetDatasetsAsync();

        foreach (var dataset in datasets.Where(d => d.Enabled))
        {
            await ScheduleDatasetJobAsync(dataset.DatasetId, dataset.CronExpression, cancellationToken);
        }

        _logger.LogInformation("Scheduled {Count} dataset ingestion jobs",
            datasets.Count(d => d.Enabled));
    }

    private async Task ScheduleDatasetJobAsync(string datasetId, string cronExpression, CancellationToken cancellationToken)
    {
        try
        {
            var jobKey = new JobKey(datasetId, "DataIngestion");

            // Check if job already exists
            if (await _scheduler!.CheckExists(jobKey, cancellationToken))
            {
                _logger.LogInformation("Job for dataset {DatasetId} already exists.  Deleting and rescheduling.",datasetId);
                await _scheduler.DeleteJob(jobKey, cancellationToken);
            }

            var job = JobBuilder.Create<DataIngestionJob>()
                .WithIdentity(jobKey)
                .UsingJobData("DatasetId", datasetId)
                .Build();

            var trigger = TriggerBuilder.Create()
                .WithIdentity($"{datasetId}-trigger", "DataIngestion")
                .WithCronSchedule(cronExpression)
                .Build();

            await _scheduler!.ScheduleJob(job, trigger, cancellationToken);

            _logger.LogInformation("Scheduled job for dataset {DatasetId} with cron: {Cron}",
                datasetId, cronExpression);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to schedule job for dataset {DatasetId}", datasetId);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping Job Scheduling Service");

        if (_scheduler != null)
        {
            await _scheduler.Shutdown(cancellationToken);
        }
    }
}
