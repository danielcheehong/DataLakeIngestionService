using System.Data;
using System.Diagnostics;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Trace;

namespace DataLakeIngestionService.Infrastructure.Transformation;

public class TransformationEngine : ITransformationEngine
{
    private readonly ILogger<TransformationEngine> _logger;
    private readonly IHostEnvironment _hostEnvironment;

    public TransformationEngine(
        ILogger<TransformationEngine> logger,
        IHostEnvironment hostEnvironment)
    {
        _logger = logger;
        _hostEnvironment = hostEnvironment;
    }

    public async Task ApplyTransformationsAsync(
        IPipelineContext context,
        List<ITransformationStep> steps,
        CancellationToken cancellationToken)
    {
        var currentEnvironment = _hostEnvironment.EnvironmentName;
        
        _logger.LogInformation(
            "Applying {Count} transformation steps in environment: {Environment}", 
            steps.Count, 
            currentEnvironment);

        foreach (var step in steps)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var stepActivity = PipelineActivitySource.Source.StartActivity($"transformation.step.{step.Name}");
            stepActivity?.SetTag("step.name", step.Name);

            // Check if step should execute in current environment
            if (!ShouldExecuteInEnvironment(step, currentEnvironment))
            {
                _logger.LogInformation(
                    "Skipping transformation '{StepName}' - not configured for environment '{Environment}'. Allowed: [{Allowed}]",
                    step.Name,
                    currentEnvironment,
                    step.Environments?.Count > 0 ? string.Join(", ", step.Environments) : "ALL");
                stepActivity?.SetTag("step.skipped", true);
                continue;
            }

            _logger.LogInformation("Applying transformation step: {StepName}", step.Name);

            try
            {
                await step.TransformAsync(context, cancellationToken);
            }
            catch (Exception ex)
            {
                stepActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                stepActivity?.AddException(ex);
                throw;
            }

            _logger.LogDebug(
                "Transformation step {StepName} completed. ExtractedData rows: {RowCount}, ExtractedDataSets count: {DataSetCount}",
                step.Name, 
                context.ExtractedData?.Rows.Count ?? 0,
                context.ExtractedDataSets.Count);
        }

        // Validate that ExtractedData is populated after all transformations
        if (context.ExtractedData == null)
        {
            throw new TransformationException(
                "ExtractedData must be populated after all transformation steps complete. " +
                "For multi-source datasets, ensure a merge/consolidation step is configured as the final transformation.");
        }
    }

    private bool ShouldExecuteInEnvironment(ITransformationStep step, string currentEnvironment)
    {
        // Null or empty list means execute in ALL environments
        if (step.Environments == null || step.Environments.Count == 0)
        {
            return true;
        }

        // Case-insensitive comparison
        return step.Environments.Any(env => 
            string.Equals(env, currentEnvironment, StringComparison.OrdinalIgnoreCase));
    }
}
