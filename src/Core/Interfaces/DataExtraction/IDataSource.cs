using System.Data;
using DataLakeIngestionService.Core.Enums;

namespace DataLakeIngestionService.Core.Interfaces.DataExtraction;

public interface IDataSource
{
    Task<DataTable> ExtractAsync(
        string connectionString,
        string query,
        ExtractionType extractionType,
        Dictionary<string, object>? parameters,
        CancellationToken cancellationToken);
}

public interface IDataSourceFactory
{
    IDataSource Create(string sourceType);
}
