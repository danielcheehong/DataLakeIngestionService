using System.Data;
using DataLakeIngestionService.Core.Pipeline;

namespace DataLakeIngestionService.Core.Interfaces.Transformation;

public interface ITransformationStep
{
    string Name { get; }
    
    /// <summary>
    /// List of environments where this transformation should execute.
    /// Empty or null list means execute in ALL environments.
    /// </summary>
    List<string> Environments { get; set; }
    
    /// <summary>
    /// Transforms data in the pipeline context.
    /// For single-source datasets: operate on context.ExtractedData.
    /// For multi-source datasets: access context.ExtractedDataSets and set context.ExtractedData when consolidating.
    /// </summary>
    Task TransformAsync(IPipelineContext context, CancellationToken cancellationToken);
}

public interface ITransformationEngine
{
    /// <summary>
    /// Applies all transformation steps to the pipeline context.
    /// After completion, context.ExtractedData must be populated.
    /// </summary>
    Task ApplyTransformationsAsync(
        IPipelineContext context,
        List<ITransformationStep> steps,
        CancellationToken cancellationToken);
}

public interface ITransformationStepFactory
{
    ITransformationStep Create(string stepName, Dictionary<string, object>? config = null);
    IEnumerable<string> GetAvailableSteps();
}
 