using System.Diagnostics;
using DataLakeIngestionService.Core.Enums;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Trace;

namespace DataLakeIngestionService.Core.Pipeline;

public abstract class BasePipelineHandler : IPipelineHandler
{
    private IPipelineHandler? _nextHandler;
    protected readonly ILogger Logger;

    protected BasePipelineHandler(ILogger logger)
    {
        Logger = logger;
    }

    public abstract string StageName { get; }

    public IPipelineHandler? SetNext(IPipelineHandler handler)
    {
        _nextHandler = handler;
        return handler;
    }

    public async Task<PipelineResult> HandleAsync(IPipelineContext context)
    {
        // Check if context already has critical errors
        if (context.Errors.Any(e => e.Severity == ErrorSeverity.Critical))
        {
            Logger.LogWarning("Pipeline aborted due to critical error in previous stage");
            return new PipelineResult
            {
                IsSuccess = false,
                Message = "Pipeline aborted due to critical error in previous stage",
                ShouldContinue = false
            };
        }

        PipelineResult result;

        // Span is scoped to this stage's work only; ends before the next handler starts.
        using (var activity = PipelineActivitySource.Source.StartActivity($"pipeline.stage.{StageName}"))
        {
            try
            {
                Logger.LogInformation("Starting stage: {StageName}", StageName);

                // Execute the stage-specific logic
                result = await ExecuteAsync(context);

                // Log metrics
                LogStageMetrics(context, result);

                // Propagate key metrics as span tags
                foreach (var kvp in result.StageMetrics)
                    activity?.SetTag($"stage.{kvp.Key.ToLowerInvariant()}", kvp.Value?.ToString());

                if (!result.IsSuccess)
                    activity?.SetStatus(ActivityStatusCode.Error, result.Message);

                Logger.LogInformation("Completed stage: {StageName} - Success: {Success}",
                    StageName, result.IsSuccess);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Unhandled exception in stage: {StageName}", StageName);

                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddException(ex);

                // Handle unexpected errors
                context.Errors.Add(new PipelineError
                {
                    Stage = StageName,
                    Message = $"Unhandled exception in {StageName}",
                    Exception = ex,
                    Timestamp = DateTime.UtcNow,
                    Severity = ErrorSeverity.Critical
                });

                result = new PipelineResult
                {
                    IsSuccess = false,
                    Message = ex.Message,
                    ShouldContinue = false
                };
            }
        } // span ends here — before the chain continues to the next handler

        // Continue to next handler if successful and should continue
        if (result.ShouldContinue && _nextHandler != null)
        {
            return await _nextHandler.HandleAsync(context);
        }

        return result;
    }

    protected abstract Task<PipelineResult> ExecuteAsync(IPipelineContext context);

    protected virtual void LogStageMetrics(IPipelineContext context, PipelineResult result)
    {
        if (result.StageMetrics.Any())
        {
            Logger.LogInformation("[{StageName}] Metrics: {Metrics}", 
                StageName, 
                string.Join(", ", result.StageMetrics.Select(kvp => $"{kvp.Key}={kvp.Value}")));
        }
    }
}
