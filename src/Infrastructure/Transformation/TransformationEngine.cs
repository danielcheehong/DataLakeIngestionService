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

        foreach (var step in steps.OrderBy(s => s.Name))
        {
            _logger.LogInformation("Applying transformation step: {StepName}", step.Name);
            transformedData = await step.TransformAsync(transformedData, cancellationToken);
        }

        return transformedData;
    }
}

// Basic transformation step examples
public class DataCleansingStep : ITransformationStep
{
    public string Name => "DataCleansing";

    public Task<DataTable> TransformAsync(DataTable data, CancellationToken cancellationToken)
    {
        // Implement data cleansing logic (trim whitespace, remove null bytes, etc.)
        foreach (DataRow row in data.Rows)
        {
            foreach (DataColumn column in data.Columns)
            {
                if (row[column] is string strValue)
                {
                    row[column] = strValue.Trim();
                }
            }
        }

        return Task.FromResult(data);
    }
}

public class DataValidationStep : ITransformationStep
{
    public string Name => "DataValidation";

    public Task<DataTable> TransformAsync(DataTable data, CancellationToken cancellationToken)
    {
        // Implement validation logic
        // For now, just return the data as-is
        return Task.FromResult(data);
    }
}
