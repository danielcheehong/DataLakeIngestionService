using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using DataLakeIngestionService.Core.Models;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Logging;
using Quartz;

namespace DataLakeIngestionService.Worker.Jobs;

[DisallowConcurrentExecution]
public class DataIngestionJob : IJob
{
    private readonly ILogger<DataIngestionJob> _logger;
    private readonly IDatasetConfigurationService _configService;
    private readonly ITransformationStepFactory _transformationStepFactory;
    private readonly DataPipeline _pipeline;
    private readonly IConfiguration _configuration;

    public DataIngestionJob(
        ILogger<DataIngestionJob> logger,
        IDatasetConfigurationService configService,
        ITransformationStepFactory transformationStepFactory,
        DataPipeline pipeline,
        IConfiguration configuration)
    {
        _logger = logger;
        _configService = configService;
        _transformationStepFactory = transformationStepFactory;
        _pipeline = pipeline;
        _configuration = configuration;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        var datasetId = context.JobDetail.JobDataMap.GetString("DatasetId");

        try
        {
            _logger.LogInformation("Starting ingestion for dataset: {DatasetId}", datasetId);

            var config = await _configService.GetDatasetByIdAsync(datasetId!);
            if (config == null)
            {
                _logger.LogWarning("Dataset configuration not found: {DatasetId}", datasetId);
                return;
            }

            if (!config.Enabled)
            {
                _logger.LogInformation("Dataset is disabled: {DatasetId}", datasetId);
                return;
            }

            // Get connection string from configuration
            var connectionString = _configuration.GetConnectionString(config.Source.ConnectionStringKey);
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogError("Connection string not found: {Key}", config.Source.ConnectionStringKey);
                throw new InvalidOperationException($"Connection string not found: {config.Source.ConnectionStringKey}");
            }

            // Build query from configuration
            var query = config.Source.ExtractionType == Core.Enums.ExtractionType.Package
                ? $"{config.Source.PackageName}.{config.Source.ProcedureName}"
                : config.Source.ProcedureName;

            // Generate file name from pattern
            var fileName = GenerateFileName(config.Parquet.FileNamePattern);

            // Build transformation steps from dataset configuration
            var transformationSteps = BuildTransformationSteps(config);

            // Build pipeline metadata
            var metadata = new Dictionary<string, object>
            {
                ["SourceType"] = config.Source.Type.ToString(),
                ["ConnectionString"] = connectionString,
                ["Query"] = query,
                ["Parameters"] = config.Source.Parameters,
                ["TransformationSteps"] = transformationSteps,
                ["UploadProvider"] = config.Upload.Provider.ToString(),
                ["DestinationPath"] = config.Upload.FileSystemConfig?.RelativePath ?? config.Upload.AzureBlobConfig?.BlobPath ?? "",
                ["FileName"] = fileName
            };

            // Execute pipeline
            var result = await _pipeline.ExecuteAsync(metadata, context.CancellationToken);

            if (result.IsSuccess)
            {
                _logger.LogInformation(
                    "Successfully completed ingestion for dataset: {DatasetId}, Duration: {Duration}s, Upload: {Uri}",
                    datasetId, result.TotalDuration.TotalSeconds, result.UploadUri);
            }
            else
            {
                _logger.LogError(
                    "Ingestion failed for dataset: {DatasetId}, Errors: {ErrorCount}",
                    datasetId, result.Errors.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed ingestion for dataset: {DatasetId}", datasetId);
            throw new JobExecutionException(ex, refireImmediately: false);
        }
    }

    private static string GenerateFileName(string pattern)
    {
        var now = DateTime.UtcNow;
        return pattern
            .Replace("{date:yyyyMMdd}", now.ToString("yyyyMMdd"))
            .Replace("{time:HHmmss}", now.ToString("HHmmss"))
            .Replace("{date}", now.ToString("yyyyMMdd"))
            .Replace("{time}", now.ToString("HHmmss"));
    }

    private List<ITransformationStep> BuildTransformationSteps(DatasetConfiguration config)
    {
        var transformationSteps = new List<ITransformationStep>();
        
        if (config.Transformations?.Any() != true)
        {
            _logger.LogInformation("No transformations configured for dataset: {DatasetId}", 
                config.DatasetId);
            return transformationSteps;
        }

        _logger.LogInformation("Loading {Count} transformation steps for dataset: {DatasetId}", 
            config.Transformations.Count(t => t.Enabled), 
            config.DatasetId);

        foreach (var transformConfig in config.Transformations
            .Where(t => t.Enabled)
            .OrderBy(t => t.Order))
        {
            try
            {
                var step = _transformationStepFactory.Create(
                    transformConfig.Type,
                    transformConfig.Config);
                
                transformationSteps.Add(step);
                
                _logger.LogDebug(
                    "Loaded transformation step: {Type} (order: {Order}) for dataset: {DatasetId}", 
                    transformConfig.Type, 
                    transformConfig.Order,
                    config.DatasetId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, 
                    "Failed to create transformation step '{Type}' for dataset: {DatasetId}", 
                    transformConfig.Type, 
                    config.DatasetId);
                throw;
            }
        }

        return transformationSteps;
    }
}
