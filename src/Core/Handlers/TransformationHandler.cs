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
        if (context.ExtractedData == null || context.ExtractedData.Rows.Count == 0)
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
                return new PipelineResult
                {
                    IsSuccess = true,
                    Message = "No transformation steps configured",
                    ShouldContinue = true
                };
            }

            // Apply transformations
            var transformedData = await _transformationEngine.ApplyTransformationsAsync(
                context.ExtractedData,
                transformationSteps,
                context.CancellationToken);

            // Update context with transformed data
            context.ExtractedData = transformedData;

            stopwatch.Stop();

            Logger.LogInformation(
                "Transformed {RowCount} rows in {ElapsedMs}ms",
                transformedData.Rows.Count,
                stopwatch.ElapsedMilliseconds);

            return new PipelineResult
            {
                IsSuccess = true,
                Message = $"Transformed {transformedData.Rows.Count} rows",
                ShouldContinue = true,
                StageMetrics = new Dictionary<string, object>
                {
                    ["RowCount"] = transformedData.Rows.Count,
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
