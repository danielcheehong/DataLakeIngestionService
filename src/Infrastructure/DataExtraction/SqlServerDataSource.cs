using System.Data;
using Dapper;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.DataExtraction;

public class SqlServerDataSource : IDataSource
{
    private readonly ILogger<SqlServerDataSource> _logger;

    public SqlServerDataSource(ILogger<SqlServerDataSource> logger)
    {
        _logger = logger;
    }

    public async Task<DataTable> ExtractAsync(
        string connectionString,
        string query,
        Dictionary<string, object>? parameters,
        CancellationToken cancellationToken)
    {
        try
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var dynamicParams = new DynamicParameters();
            if (parameters != null)
            {
                foreach (var param in parameters)
                {
                    dynamicParams.Add(param.Key, param.Value);
                }
            }

            _logger.LogInformation("Executing SQL Server query: {Query}", query);

            // Execute and get data reader
            var reader = await connection.ExecuteReaderAsync(
                query,
                dynamicParams,
                commandType: CommandType.StoredProcedure,
                commandTimeout: 300);

            // Convert to DataTable
            var dataTable = new DataTable();
            dataTable.Load(reader);

            _logger.LogInformation("Retrieved {RowCount} rows from SQL Server", dataTable.Rows.Count);

            return dataTable;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to extract data from SQL Server");
            throw new ExtractionException($"SQL Server extraction failed: {ex.Message}", ex);
        }
    }
}
