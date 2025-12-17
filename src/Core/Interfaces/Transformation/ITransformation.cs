using System.Data;

namespace DataLakeIngestionService.Core.Interfaces.Transformation;

public interface ITransformationStep
{
    string Name { get; }
    Task<DataTable> TransformAsync(DataTable data, CancellationToken cancellationToken);
}

public interface ITransformationEngine
{
    Task<DataTable> ApplyTransformationsAsync(
        DataTable data,
        List<ITransformationStep> steps,
        CancellationToken cancellationToken);
}
