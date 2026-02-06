using System.Diagnostics;
using DataLakeIngestionService.Core.Enums;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Core.Handlers;

public class TransformationHandler : BasePipelineHandler
{
    private readonly ITransformationEngine _transformationEngine;

    public TransformationHandler(
        ITransformationEngine transformationEngine,
        ILogger<TransformationHandler> logger) : base(logger)
    {
        _transformationEngine = transformationEngine;
    }

    public override string StageName => "Transformation";

    protected override async Task<PipelineResult> ExecuteAsync(IPipelineContext context)
    {
        // Check if there's any data to transform (single source or multi-source)
        var hasExtractedData = context.ExtractedData != null && context.ExtractedData.Rows.Count > 0;
        var hasExtractedDataSets = context.ExtractedDataSets.Count > 0;
        
        if (!hasExtractedData && !hasExtractedDataSets)
        {
            Logger.LogWarning("No data to transform");
            return new PipelineResult
            {
                IsSuccess = true,
                Message = "No data to transform",
                ShouldContinue = true
            };
        }

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Get transformation steps from context
            var transformationSteps = context.Metadata.TryGetValue("TransformationSteps", out var steps)
                ? steps as List<ITransformationStep>
                : new List<ITransformationStep>();

            if (transformationSteps == null || !transformationSteps.Any())
            {
                Logger.LogInformation("No transformation steps configured, skipping transformation");
                
                // For multi-source with no transformations, we still need ExtractedData populated
                if (!hasExtractedData && hasExtractedDataSets)
                {
                    throw new TransformationException(
                        "Multi-source dataset requires transformation steps to consolidate ExtractedDataSets into ExtractedData.");
                }
                
                return new PipelineResult
                {
                    IsSuccess = true,
                    Message = "No transformation steps configured",
                    ShouldContinue = true
                };
            }

            // Apply transformations - engine modifies context directly
            await _transformationEngine.ApplyTransformationsAsync(
                context,
                transformationSteps,
                context.CancellationToken);

            stopwatch.Stop();

            Logger.LogInformation(
                "Transformed data in {ElapsedMs}ms. Final row count: {RowCount}",
                stopwatch.ElapsedMilliseconds,
                context.ExtractedData?.Rows.Count ?? 0);

            return new PipelineResult
            {
                IsSuccess = true,
                Message = $"Transformed {context.ExtractedData?.Rows.Count ?? 0} rows",
                ShouldContinue = true,
                StageMetrics = new Dictionary<string, object>
                {
                    ["RowCount"] = context.ExtractedData?.Rows.Count ?? 0,
                    ["DurationMs"] = stopwatch.ElapsedMilliseconds
                }
            };
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Transformation failed");

            context.Errors.Add(new PipelineError
            {
                Stage = StageName,
                Message = "Data transformation failed",
                Exception = ex,
                Timestamp = DateTime.UtcNow,
                Severity = ErrorSeverity.Critical
            });

            return new PipelineResult
            {
                IsSuccess = false,
                Message = ex.Message,
                ShouldContinue = false
            };
        }
    }
}
