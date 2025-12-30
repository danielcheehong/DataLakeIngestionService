using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Core.Pipeline;

public class DataPipeline
{
    private readonly IPipelineHandler _firstHandler;
    private readonly ILogger<DataPipeline> _logger;

    public DataPipeline(
        ILogger<DataPipeline> logger,
        IPipelineHandler extractionHandler,
        IPipelineHandler transformationHandler,
        IPipelineHandler parquetHandler,
        IPipelineHandler uploadHandler)
    {
        _logger = logger;

        // Build the chain: Extraction → Transformation → Parquet → Upload
        _firstHandler = extractionHandler;
        extractionHandler
            .SetNext(transformationHandler)
            ?.SetNext(parquetHandler)
            ?.SetNext(uploadHandler);
    }

    public async Task<PipelineExecutionResult> ExecuteAsync(
        Dictionary<string, object> metadata,
        string jobId,
        CancellationToken cancellationToken = default)
    {
        var context = new PipelineContext
        {
            JobId = jobId,
            StartTime = DateTime.UtcNow,
            Metadata = metadata,
            CancellationToken = cancellationToken
        };

        _logger.LogInformation("Starting pipeline execution for job {JobId}", context.JobId);

        var result = await _firstHandler.HandleAsync(context);

        var totalDuration = DateTime.UtcNow - context.StartTime;

        _logger.LogInformation(
            "Pipeline execution completed for job {JobId} - Success: {Success}, Duration: {Duration}s",
            context.JobId,
            result.IsSuccess,
            totalDuration.TotalSeconds);

        return new PipelineExecutionResult
        {
            JobId = context.JobId,
            IsSuccess = result.IsSuccess,
            Message = result.Message,
            Errors = context.Errors,
            TotalDuration = totalDuration,
            UploadUri = context.UploadUri
        };
    }
}
