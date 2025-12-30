using System.Data;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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

    public async Task<DataTable> ApplyTransformationsAsync(
        DataTable data,
        List<ITransformationStep> steps,
        CancellationToken cancellationToken)
    {
        var transformedData = data.Copy();
        var currentEnvironment = _hostEnvironment.EnvironmentName;
        
        _logger.LogInformation(
            "Applying {Count} transformation steps in environment: {Environment}", 
            steps.Count, 
            currentEnvironment);

        foreach (var step in steps)
        {
            // Check if step should execute in current environment
            if (!ShouldExecuteInEnvironment(step, currentEnvironment))
            {
                _logger.LogInformation(
                    "Skipping transformation '{StepName}' - not configured for environment '{Environment}'. Allowed: [{Allowed}]",
                    step.Name,
                    currentEnvironment,
                    step.Environments?.Count > 0 ? string.Join(", ", step.Environments) : "ALL");
                continue;
            }

            _logger.LogInformation("Applying transformation step: {StepName}", step.Name);
            
            transformedData = await step.TransformAsync(transformedData, cancellationToken);
            
            _logger.LogDebug("Transformation step {StepName} completed. Row count: {RowCount}",
                step.Name, transformedData.Rows.Count);
        }

        return transformedData;
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
