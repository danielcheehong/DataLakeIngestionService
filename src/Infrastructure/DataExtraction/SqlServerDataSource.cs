using System.Data;
using System.Text.Json;
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

            if (parameters != null && parameters.Count > 0)
            {
                foreach (var param in parameters)
                {
                    // Convert JsonElement if needed
                    var value = ConvertParameterValue(param.Value);
                    
                    _logger.LogDebug("Adding parameter {Name} = {Value} (Type: {Type})", 
                        param.Key, value, value?.GetType().Name ?? "null");
                    
                    dynamicParams.Add(param.Key, value);
                }
            }

            var reader = await connection.ExecuteReaderAsync(
                query,
                dynamicParams,
                commandType: CommandType.StoredProcedure,
                commandTimeout: 600);

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

    /// <summary>
    /// Converts parameter value from JsonElement to native type if needed
    /// </summary>
    private object? ConvertParameterValue(object? value)
    {
        if (value is JsonElement jsonElement)
        {
            return jsonElement.ValueKind switch
            {
                JsonValueKind.String => jsonElement.GetString(),
                JsonValueKind.Number => jsonElement.TryGetInt32(out var intValue) ? intValue :
                                       jsonElement.TryGetInt64(out var longValue) ? longValue :
                                       jsonElement.TryGetDecimal(out var decimalValue) ? decimalValue :
                                       jsonElement.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                _ => jsonElement.GetRawText()
            };
        }

        return value;
    }
}