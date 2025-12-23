using System.Data;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Transformation;

public class TransformationEngine : ITransformationEngine
{
    private readonly ILogger<TransformationEngine> _logger;

    public TransformationEngine(ILogger<TransformationEngine> logger)
    {
        _logger = logger;
    }

    public async Task<DataTable> ApplyTransformationsAsync(
        DataTable data,
        List<ITransformationStep> steps,
        CancellationToken cancellationToken)
    {
        var transformedData = data.Copy();

        foreach (var step in steps)
        {
            _logger.LogInformation("Applying transformation step: {StepName}", step.Name);
            
            transformedData = await step.TransformAsync(transformedData, cancellationToken);
            
            _logger.LogDebug("Transformation step {StepName} completed. Row count: {RowCount}",
                step.Name, transformedData.Rows.Count);
        }

        return transformedData;
    }
}
