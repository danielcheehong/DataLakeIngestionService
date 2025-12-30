using System.Data;

namespace DataLakeIngestionService.Core.Interfaces.Transformation;

public interface ITransformationStep
{
    string Name { get; }
    
    /// <summary>
    /// List of environments where this transformation should execute.
    /// Empty or null list means execute in ALL environments.
    /// </summary>
    List<string> Environments { get; set; }
    
    Task<DataTable> TransformAsync(DataTable data, CancellationToken cancellationToken);
}

public interface ITransformationEngine
{
    Task<DataTable> ApplyTransformationsAsync(
        DataTable data,
        List<ITransformationStep> steps,
        CancellationToken cancellationToken);
}

public interface ITransformationStepFactory
{
    ITransformationStep Create(string stepName, Dictionary<string, object>? config = null);
    IEnumerable<string> GetAvailableSteps();
}
 