using System.Data;

namespace DataLakeIngestionService.Core.Interfaces.DataExtraction;

public interface IDataSource
{
    Task<DataTable> ExtractAsync(
        string connectionString,
        string query,
        Dictionary<string, object>? parameters,
        CancellationToken cancellationToken);
}

public interface IDataSourceFactory
{
    IDataSource Create(string sourceType);
}
