using System.Data;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;

namespace DataLakeIngestionService.Infrastructure.DataExtraction;

public class OracleDataSource : IDataSource
{
    private const int _cmdTimeout = 600;
    private readonly ILogger<OracleDataSource> _logger;

    public OracleDataSource(ILogger<OracleDataSource> logger)
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
            using var connection = new OracleConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Check if query is a package call
            if (query.Contains("."))
            {
                return await ExecutePackageProcedureAsync(connection, query, parameters, cancellationToken);
            }
            else
            {
                return await ExecuteQueryAsync(connection, query, parameters, cancellationToken);
            }
        }
        catch (OracleException ex)
        {
            _logger.LogError(ex, "Oracle error during extraction. Error code: {ErrorCode}, Message: {Message}", 
                ex.Number, ex.Message);
            throw new ExtractionException($"Oracle extraction failed: {ex.Message}", ex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to extract data from Oracle");
            throw new ExtractionException($"Oracle extraction failed: {ex.Message}", ex);
        }
    }

    private async Task<DataTable> ExecutePackageProcedureAsync(
        OracleConnection connection,
        string packageProcedure,
        Dictionary<string, object>? parameters,
        CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandType = CommandType.StoredProcedure;
        command.CommandText = packageProcedure;
        command.CommandTimeout = _cmdTimeout;

        _logger.LogInformation("Executing Oracle package procedure: {Procedure}", packageProcedure);

        // Add input parameters IN ORDER from the dictionary
        if (parameters != null && parameters.Count > 0)
        {
            foreach (var param in parameters)
            {
                var paramName = param.Key.TrimStart(':');
                var oracleParam = new OracleParameter
                {
                    ParameterName = paramName,
                    Value = param.Value ?? DBNull.Value,
                    Direction = ParameterDirection.Input
                };
                command.Parameters.Add(oracleParam);

                _logger.LogDebug("Added input parameter: {Name} = {Value} (Type: {Type})", 
                    paramName, param.Value, param.Value?.GetType().Name ?? "null");
            }
        }

        // Add output REF CURSOR parameter - MUST BE LAST
        var cursorParam = new OracleParameter
        {
            ParameterName = "p_cursor",
            OracleDbType = OracleDbType.RefCursor,
            Direction = ParameterDirection.Output
        };
        command.Parameters.Add(cursorParam);

        _logger.LogDebug("Added output cursor parameter: p_cursor");
        _logger.LogDebug("Total parameters: {Count}", command.Parameters.Count);

        // Execute the stored procedure
        await command.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogDebug("Command executed successfully. Retrieving REF CURSOR...");

        // Read data from REF CURSOR
        var dataTable = new DataTable();
        
        if (cursorParam.Value is Oracle.ManagedDataAccess.Types.OracleRefCursor refCursor)
        {
            using var reader = refCursor.GetDataReader();
            dataTable.Load(reader);
            
            // Log DataTable schema for debugging type mismatches
            _logger.LogDebug("Loaded DataTable schema from Oracle ({ColumnCount} columns, {RowCount} rows):", 
                dataTable.Columns.Count, dataTable.Rows.Count);
            
            foreach (DataColumn col in dataTable.Columns)
            {
                var sampleValue = dataTable.Rows.Count > 0 ? dataTable.Rows[0][col] : null;
                var sampleValueType = sampleValue == DBNull.Value ? "DBNull" : sampleValue?.GetType().Name ?? "null";
                var sampleValueStr = sampleValue == DBNull.Value ? "DBNull" : sampleValue?.ToString() ?? "null";
                
                _logger.LogDebug("  Column '{Name}': DataType={Type}, AllowDBNull={AllowNull}, Sample={Sample} (ValueType={ValueType})", 
                    col.ColumnName, 
                    col.DataType.Name, 
                    col.AllowDBNull,
                    sampleValueStr,
                    sampleValueType);
            }
            
            _logger.LogInformation("Successfully retrieved {RowCount} rows from Oracle procedure {Procedure}", 
                dataTable.Rows.Count, packageProcedure);
        }
        else
        {
            throw new ExtractionException(
                $"Expected OracleRefCursor but got {cursorParam.Value?.GetType().Name ?? "null"}");
        }

        return dataTable;
    }

    private async Task<DataTable> ExecuteQueryAsync(
        OracleConnection connection,
        string sqlQuery,
        Dictionary<string, object>? parameters,
        CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = sqlQuery;
        command.CommandTimeout = _cmdTimeout;

        _logger.LogInformation("Executing Oracle query: {Query}", sqlQuery);

        // Add parameters for direct SQL query
        if (parameters != null)
        {
            foreach (var param in parameters)
            {
                var paramName = param.Key.TrimStart(':');
                var oracleParam = new OracleParameter
                {
                    ParameterName = paramName,
                    Value = param.Value ?? DBNull.Value
                };
                command.Parameters.Add(oracleParam);

                _logger.LogDebug("Added parameter: {Name} = {Value}", paramName, param.Value);
            }
        }

        var dataTable = new DataTable();
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        dataTable.Load(reader);

        // Log DataTable schema for SQL queries too
        _logger.LogDebug("Loaded DataTable schema from Oracle query ({ColumnCount} columns, {RowCount} rows):", 
            dataTable.Columns.Count, dataTable.Rows.Count);
        
        foreach (DataColumn col in dataTable.Columns)
        {
            var sampleValue = dataTable.Rows.Count > 0 ? dataTable.Rows[0][col] : null;
            var sampleValueType = sampleValue == DBNull.Value ? "DBNull" : sampleValue?.GetType().Name ?? "null";
            
            _logger.LogDebug("  Column '{Name}': DataType={Type}, AllowDBNull={AllowNull}, ValueType={ValueType}", 
                col.ColumnName, 
                col.DataType.Name, 
                col.AllowDBNull,
                sampleValueType);
        }

        _logger.LogInformation("Retrieved {RowCount} rows from Oracle query", dataTable.Rows.Count);

        return dataTable;
    }
}