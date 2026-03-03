using DataLakeIngestionService.Core.Interfaces.DataExtraction;
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
    private static readonly System.Text.Json.JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
    };

    private readonly ILogger<DataIngestionJob> _logger;
    private readonly IDatasetConfigurationService _configService;
    private readonly ITransformationStepFactory _transformationStepFactory;
    private readonly IConnectionStringBuilder _connectionStringBuilder;
    private readonly IParameterResolverService _parameterResolver;
    private readonly DataPipeline _pipeline;
    private readonly IConfiguration _configuration;

    public DataIngestionJob(
        ILogger<DataIngestionJob> logger,
        IDatasetConfigurationService configService,
        ITransformationStepFactory transformationStepFactory,
        IConnectionStringBuilder connectionStringBuilder,
        IParameterResolverService parameterResolver,
        DataPipeline pipeline,
        IConfiguration configuration)
    {
        _logger = logger;
        _configService = configService;
        _transformationStepFactory = transformationStepFactory;
        _connectionStringBuilder = connectionStringBuilder;
        _parameterResolver = parameterResolver;
        _pipeline = pipeline;
        _configuration = configuration;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        var datasetId = context.JobDetail.JobDataMap.GetString("DatasetId");
        var isRunOnce = context.JobDetail.JobDataMap.ContainsKey("IsRunOnce")
                        && context.JobDetail.JobDataMap.GetString("IsRunOnce") == "true";

        // Generate unique execution ID: datasetId.timestamp-shortGuid
        var executionId = $"{datasetId}.{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid().ToString("N")[..8]}";

        try
        {
            _logger.LogInformation("Starting ingestion for dataset: {DatasetId}, ExecutionId: {ExecutionId}",
                datasetId, executionId);

            // For run-once jobs use the config embedded in the job data map; otherwise read from disk
            DatasetConfiguration? config;
            var configOverrideJson = context.MergedJobDataMap.ContainsKey("ConfigOverride")
                ? context.MergedJobDataMap.GetString("ConfigOverride")
                : null;
            if (!string.IsNullOrEmpty(configOverrideJson))
            {
                config = System.Text.Json.JsonSerializer.Deserialize<DatasetConfiguration>(configOverrideJson, _jsonOptions);
                _logger.LogInformation("Using config override from job data map for dataset: {DatasetId}, ExecutionId: {ExecutionId}",
                    datasetId, executionId);
            }
            else
            {
                config = await _configService.GetDatasetByIdAsync(datasetId!);
            }

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

            // Generate file name from pattern
            var fileName = GenerateFileName(config.Parquet.FileNamePattern);

            // Build transformation steps from dataset configuration
            var transformationSteps = BuildTransformationSteps(config, executionId);

            // Build pipeline metadata based on single or multi-source configuration
            Dictionary<string, object> metadata;

            if (config.HasMultipleSources)
            {
                // Multi-source: build source configurations list
                var sourceConfigs = await BuildSourceConfigurationsAsync(config, context.CancellationToken);
                
                metadata = new Dictionary<string, object>
                {
                    ["DatasetId"] = datasetId!,
                    ["ExecutionId"] = executionId,
                    ["SourceConfigurations"] = sourceConfigs,
                    ["TransformationSteps"] = transformationSteps,
                    ["UploadProvider"] = config.Upload.Provider.ToString(),
                    ["DestinationPath"] = config.Upload.FileSystemConfig?.RelativePath 
                                          ?? config.Upload.AzureBlobConfig?.BlobPath ?? "",
                    ["FileName"] = fileName,
                    ["KeepLocalCopy"] = config.Upload.KeepLocalCopy,
                    ["LocalCopyPath"] = config.Upload.LocalCopyPath ?? string.Empty
                };
                
                _logger.LogInformation(
                    "Built metadata for multi-source dataset with {SourceCount} sources: {DatasetId}, ExecutionId: {ExecutionId}",
                    sourceConfigs.Count, datasetId, executionId);
            }
            else
            {
                // Single source: existing logic (backward compatibility)
                metadata = await BuildSingleSourceMetadataAsync(config, datasetId!, executionId, fileName, transformationSteps, context.CancellationToken);
            }

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
        finally
        {
            // Self-delete the temporary job after execution (success or failure)
            if (isRunOnce)
            {
                try
                {
                    await context.Scheduler.DeleteJob(context.JobDetail.Key);
                    _logger.LogInformation("Run-once job self-removed: {DatasetId}", datasetId);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to self-remove run-once job: {DatasetId}", datasetId);
                }
            }
        }
    }

    private async Task<Dictionary<string, object>> BuildSingleSourceMetadataAsync(
        DatasetConfiguration config,
        string datasetId,
        string executionId,
        string fileName,
        List<ITransformationStep> transformationSteps,
        CancellationToken cancellationToken)
    {
        // Get connection string template from configuration (not required for DotNet sources)
        var connectionString = string.Empty;
        if (config.Source.Type != Core.Enums.DataSourceType.DotNet)
        {
            var connectionStringTemplate = _configuration.GetConnectionString(config.Source.ConnectionStringKey);

            if (string.IsNullOrEmpty(connectionStringTemplate))
            {
                _logger.LogError("Connection string not found: {Key}, ExecutionId: {ExecutionId}", 
                    config.Source.ConnectionStringKey, executionId);
                throw new InvalidOperationException($"Connection string not found: {config.Source.ConnectionStringKey}");
            }

            // Build connection string with vault password resolution.
            connectionString = await _connectionStringBuilder.BuildConnectionStringAsync(
                connectionStringTemplate, 
                cancellationToken);
        }

        // Build query from configuration
        var query = await BuildQueryAsync(config.Source, executionId, cancellationToken);

        // Resolve runtime parameters (e.g., ${today}, ${env:VAR})
        var resolutionContext = new ParameterResolutionContext
        {
            DatasetId = datasetId,
            ExecutionTime = DateTime.UtcNow,
            Environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
        };

        var resolvedParameters = await _parameterResolver.ResolveAsync(
            config.Source.Parameters,
            resolutionContext,
            cancellationToken);

        _logger.LogDebug("Resolved {Count} parameters for dataset: {DatasetId}, ExecutionId: {ExecutionId}",
            resolvedParameters.Count, datasetId, executionId);

        return new Dictionary<string, object>
        {
            ["DatasetId"] = datasetId,
            ["ExecutionId"] = executionId,
            ["SourceType"] = config.Source.Type.ToString(),
            ["ExtractionType"] = config.Source.ExtractionType,
            ["ConnectionString"] = connectionString,
            ["Query"] = query,
            ["Parameters"] = resolvedParameters,
            ["TransformationSteps"] = transformationSteps,
            ["UploadProvider"] = config.Upload.Provider.ToString(),
            ["DestinationPath"] = config.Upload.FileSystemConfig?.RelativePath ?? config.Upload.AzureBlobConfig?.BlobPath ?? "",
            ["FileName"] = fileName,
            ["KeepLocalCopy"] = config.Upload.KeepLocalCopy,
            ["LocalCopyPath"] = config.Upload.LocalCopyPath ?? string.Empty
        };
    }

    private async Task<List<SourceExtractionConfig>> BuildSourceConfigurationsAsync(
        DatasetConfiguration config,
        CancellationToken cancellationToken)
    {
        var sourceConfigs = new List<SourceExtractionConfig>();

        foreach (var source in config.Sources!)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrEmpty(source.SourceId))
            {
                throw new InvalidOperationException(
                    $"SourceId is required for multi-source datasets. Dataset: {config.DatasetId}");
            }

            // Get connection string (not required for DotNet sources)
            var connectionString = string.Empty;
            if (source.Type != Core.Enums.DataSourceType.DotNet)
            {
                var connectionStringTemplate = _configuration.GetConnectionString(source.ConnectionStringKey);
                if (string.IsNullOrEmpty(connectionStringTemplate))
                {
                    throw new InvalidOperationException(
                        $"Connection string not found: {source.ConnectionStringKey} for source: {source.SourceId}");
                }
                connectionString = await _connectionStringBuilder.BuildConnectionStringAsync(
                    connectionStringTemplate, cancellationToken);
            }

            // Build query
            var query = await BuildQueryAsync(source, $"{config.DatasetId}.{source.SourceId}", cancellationToken);

            // Resolve parameters
            var resolutionContext = new ParameterResolutionContext
            {
                DatasetId = config.DatasetId,
                ExecutionTime = DateTime.UtcNow,
                Environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
            };
            var resolvedParameters = await _parameterResolver.ResolveAsync(
                source.Parameters, resolutionContext, cancellationToken);

            sourceConfigs.Add(new SourceExtractionConfig
            {
                SourceId = source.SourceId,
                SourceType = source.Type.ToString(),
                ExtractionType = source.ExtractionType,
                ConnectionString = connectionString,
                Query = query,
                Parameters = resolvedParameters
            });

            _logger.LogDebug(
                "Built source config for '{SourceId}' ({SourceType}), Dataset: {DatasetId}",
                source.SourceId, source.Type, config.DatasetId);
        }

        return sourceConfigs;
    }

    private async Task<string> BuildQueryAsync(SourceConfiguration source, string logContext, CancellationToken cancellationToken)
    {
        string query;
        if (source.ExtractionType == Core.Enums.ExtractionType.Query)
        {
            // Read SQL from file in Datasets/SqlFiles folder
            var sqlFilePath = Path.Combine(
                AppContext.BaseDirectory, 
                "Datasets", 
                "SqlFiles", 
                source.SqlFilePath);
            
            if (!File.Exists(sqlFilePath))
            {
                _logger.LogError("SQL file not found: {SqlFilePath}, Context: {Context}", 
                    sqlFilePath, logContext);
                throw new FileNotFoundException($"SQL file not found: {sqlFilePath}");
            }
            
            query = await File.ReadAllTextAsync(sqlFilePath, cancellationToken);
            _logger.LogDebug("Loaded SQL query from file: {SqlFilePath}, Context: {Context}", 
                source.SqlFilePath, logContext);
        }
        else if (source.ExtractionType == Core.Enums.ExtractionType.Package)
        {
            query = $"{source.PackageName}.{source.ProcedureName}";
        }
        else if (source.ExtractionType == Core.Enums.ExtractionType.CodeGenerator)
        {
            // For DotNet sources, the query is the provider name
            query = source.ProviderName;
            _logger.LogDebug("Using DotNet provider: {ProviderName}, Context: {Context}", 
                query, logContext);
        }
        else
        {
            query = source.ProcedureName;
        }

        return query;
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
