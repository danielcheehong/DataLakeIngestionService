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
    private readonly IConnectionStringBuilder _connectionStringBuilder;
    private readonly DataPipeline _pipeline;
    private readonly IConfiguration _configuration;

    public DataIngestionJob(
        ILogger<DataIngestionJob> logger,
        IDatasetConfigurationService configService,
        ITransformationStepFactory transformationStepFactory,
        IConnectionStringBuilder connectionStringBuilder,
        DataPipeline pipeline,
        IConfiguration configuration)
    {
        _logger = logger;
        _configService = configService;
        _transformationStepFactory = transformationStepFactory;
        _connectionStringBuilder = connectionStringBuilder;
        _pipeline = pipeline;
        _configuration = configuration;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        var datasetId = context.JobDetail.JobDataMap.GetString("DatasetId");
        
        // Generate unique execution ID: datasetId.timestamp-shortGuid
        var executionId = $"{datasetId}.{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid().ToString("N")[..8]}";

        try
        {
            _logger.LogInformation("Starting ingestion for dataset: {DatasetId}, ExecutionId: {ExecutionId}", 
                datasetId, executionId);
            
            // Retrieve dataset configuration from the configuration json files in the Datasets folder.
            var config = await _configService.GetDatasetByIdAsync(datasetId!);

            if (config == null)
            {
                _logger.LogWarning("Dataset configuration not found: {DatasetId}, ExecutionId: {ExecutionId}", 
                    datasetId, executionId);
                return;
            }

            // Extra layer of protection to avoid running disabled datasets. It is expected that the JobSchedulingService
            // does not schedule jobs for disabled datasets.
            if (!config.Enabled)
            {
                _logger.LogInformation("Dataset is disabled: {DatasetId}, ExecutionId: {ExecutionId}", 
                    datasetId, executionId);
                return;
            }

            // Get connection string template from configuration
            var connectionStringTemplate = _configuration.GetConnectionString(config.Source.ConnectionStringKey);

            if (string.IsNullOrEmpty(connectionStringTemplate))
            {
                _logger.LogError("Connection string not found: {Key}, ExecutionId: {ExecutionId}", 
                    config.Source.ConnectionStringKey, executionId);
                throw new InvalidOperationException($"Connection string not found: {config.Source.ConnectionStringKey}");
            }

            // Build connection string with vault password resolution.
            
            var connectionString = await _connectionStringBuilder.BuildConnectionStringAsync(
                connectionStringTemplate, 
                context.CancellationToken);

            // Build query from configuration
            string query;
            if (config.Source.ExtractionType == Core.Enums.ExtractionType.Query)
            {
                // Read SQL from file in Datasets/SqlFiles folder
                var sqlFilePath = Path.Combine(
                    AppContext.BaseDirectory, 
                    "Datasets", 
                    "SqlFiles", 
                    config.Source.SqlFilePath);
                
                if (!File.Exists(sqlFilePath))
                {
                    _logger.LogError("SQL file not found: {SqlFilePath}, ExecutionId: {ExecutionId}", 
                        sqlFilePath, executionId);
                    throw new FileNotFoundException($"SQL file not found: {sqlFilePath}");
                }
                
                query = await File.ReadAllTextAsync(sqlFilePath, context.CancellationToken);
                _logger.LogDebug("Loaded SQL query from file: {SqlFilePath}, ExecutionId: {ExecutionId}", 
                    config.Source.SqlFilePath, executionId);
            }
            else if (config.Source.ExtractionType == Core.Enums.ExtractionType.Package)
            {
                query = $"{config.Source.PackageName}.{config.Source.ProcedureName}";
            }
            else
            {
                query = config.Source.ProcedureName;
            }

            // Generate file name from pattern
            var fileName = GenerateFileName(config.Parquet.FileNamePattern);

            // Build transformation steps from dataset configuration
            var transformationSteps = BuildTransformationSteps(config, executionId);

            // Build pipeline metadata
            var metadata = new Dictionary<string, object>
            {
                ["DatasetId"] = datasetId!,
                ["ExecutionId"] = executionId,
                ["SourceType"] = config.Source.Type.ToString(),
                ["ConnectionString"] = connectionString,
                ["Query"] = query,
                ["Parameters"] = config.Source.Parameters,
                ["TransformationSteps"] = transformationSteps,
                ["UploadProvider"] = config.Upload.Provider.ToString(),
                ["DestinationPath"] = config.Upload.FileSystemConfig?.RelativePath ?? config.Upload.AzureBlobConfig?.BlobPath ?? "",
                ["FileName"] = fileName,
                ["KeepLocalCopy"] = config.Upload.KeepLocalCopy,
                ["LocalCopyPath"] = config.Upload.LocalCopyPath ?? string.Empty
            };

            // Execute pipeline with execution ID as JobId
            var result = await _pipeline.ExecuteAsync(metadata, executionId, context.CancellationToken);

            if (result.IsSuccess)
            {
                _logger.LogInformation(
                    "Successfully completed ingestion for dataset: {DatasetId}, ExecutionId: {ExecutionId}, Duration: {Duration}s, Upload: {Uri}",
                    datasetId, executionId, result.TotalDuration.TotalSeconds, result.UploadUri);
            }
            else
            {
                _logger.LogError(
                    "Ingestion failed for dataset: {DatasetId}, ExecutionId: {ExecutionId}, Errors: {ErrorCount}",
                    datasetId, executionId, result.Errors.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed ingestion for dataset: {DatasetId}, ExecutionId: {ExecutionId}", 
                datasetId, executionId);
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

    private List<ITransformationStep> BuildTransformationSteps(DatasetConfiguration config, string executionId)
    {
        var transformationSteps = new List<ITransformationStep>();
        
        if (config.Transformations?.Any() != true)
        {
            _logger.LogInformation("No transformations configured for dataset: {DatasetId}, ExecutionId: {ExecutionId}", 
                config.DatasetId, executionId);
            return transformationSteps;
        }

        _logger.LogInformation("Loading {Count} transformation steps for dataset: {DatasetId}, ExecutionId: {ExecutionId}", 
            config.Transformations.Count(t => t.Enabled), 
            config.DatasetId, executionId);

        foreach (var transformConfig in config.Transformations
            .Where(t => t.Enabled)
            .OrderBy(t => t.Order))
        {
            try
            {
                var step = _transformationStepFactory.Create(
                    transformConfig.Type,
                    transformConfig.Config);
                
                // Set environments from configuration
                step.Environments = transformConfig.Environments ?? new List<string>();
                
                transformationSteps.Add(step);
                
                _logger.LogDebug(
                    "Loaded transformation step: {Type} (order: {Order}, environments: [{Environments}]) for dataset: {DatasetId}, ExecutionId: {ExecutionId}", 
                    transformConfig.Type, 
                    transformConfig.Order,
                    step.Environments.Count > 0 ? string.Join(", ", step.Environments) : "ALL",
                    config.DatasetId, executionId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, 
                    "Failed to create transformation step '{Type}' for dataset: {DatasetId}, ExecutionId: {ExecutionId}", 
                    transformConfig.Type, 
                    config.DatasetId, executionId);
                throw;
            }
        }

        return transformationSteps;
    }
}
