using System.Data;
using System.Globalization;
using System.Text.Json;
using DataLakeIngestionService.Core.Enums;
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
        ExtractionType extractionType,
        Dictionary<string, object>? parameters,
        CancellationToken cancellationToken)
    {
        try
        {
            using var connection = new OracleConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            _logger.LogInformation("Executing Oracle {ExtractionType}: {Query}", 
                extractionType, 
                extractionType == ExtractionType.Query ? query.Substring(0, Math.Min(100, query.Length)) + "..." : query);
            
            return extractionType switch
            {
                ExtractionType.Query => 
                    await ExecuteQueryAsync(connection, query, parameters, cancellationToken),
                
                ExtractionType.Package => 
                    await ExecutePackageProcedureAsync(connection, query, parameters, cancellationToken),
                
                ExtractionType.StoredProcedure => 
                    await ExecuteStoredProcedureAsync(connection, query, parameters, cancellationToken),
                
                _ => throw new ArgumentException($"Unsupported extraction type: {extractionType}", nameof(extractionType))
            };
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

    /// <summary>
    /// Creates an OracleParameter with automatic type inference from the value.
    /// Handles string-to-native type conversion for dates, numbers, etc.
    /// </summary>
    private OracleParameter CreateTypedOracleParameter(string name, object? value, ParameterDirection direction = ParameterDirection.Input)
    {
        var param = new OracleParameter
        {
            ParameterName = name,
            Direction = direction
        };

        // Handle null values
        if (value == null)
        {
            param.Value = DBNull.Value;
            return param;
        }

        // Convert JsonElement to native type first
        var convertedValue = ConvertJsonElement(value);
        if (convertedValue == null)
        {
            param.Value = DBNull.Value;
            return param;
        }

        // Handle string values - attempt type inference
        if (convertedValue is string strValue)
        {
            if (int.TryParse(strValue, out int intValue))
            {
                param.OracleDbType = OracleDbType.Int32;
                param.Value = intValue;
            }
            else if (long.TryParse(strValue, out long longValue))
            {
                param.OracleDbType = OracleDbType.Int64;
                param.Value = longValue;
            }
            else if (decimal.TryParse(strValue, out decimal decimalValue))
            {
                param.OracleDbType = OracleDbType.Decimal;
                param.Value = decimalValue;
            }
            else if (DateTime.TryParseExact(strValue, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateValue))
            {
                param.OracleDbType = OracleDbType.Date;
                param.Value = dateValue;
            }
            else
            {
                param.OracleDbType = OracleDbType.Varchar2;
                param.Value = strValue;
            }
        }
        // Handle native .NET types
        else
        {
            param.OracleDbType = convertedValue switch
            {
                int => OracleDbType.Int32,
                long => OracleDbType.Int64,
                decimal => OracleDbType.Decimal,
                double => OracleDbType.Double,
                float => OracleDbType.Single,
                DateTime => OracleDbType.Date,
                bool => OracleDbType.Byte,
                byte[] => OracleDbType.Blob,
                _ => OracleDbType.Varchar2
            };
            param.Value = convertedValue is bool b ? (b ? 1 : 0) : convertedValue;
        }

        return param;
    }

    /// <summary>
    /// Converts JsonElement to native .NET type
    /// </summary>
    private static object? ConvertJsonElement(object? value)
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
                var oracleParam = CreateTypedOracleParameter(paramName, param.Value, ParameterDirection.Input);
                command.Parameters.Add(oracleParam);

                _logger.LogDebug("Added input parameter: {Name} = {Value} (OracleDbType: {DbType})", 
                    paramName, oracleParam.Value, oracleParam.OracleDbType);
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

    private async Task<DataTable> ExecuteStoredProcedureAsync(
        OracleConnection connection,
        string procedureName,
        Dictionary<string, object>? parameters,
        CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandType = CommandType.StoredProcedure;
        command.CommandText = procedureName;
        command.CommandTimeout = _cmdTimeout;

        _logger.LogInformation("Executing Oracle stored procedure: {Procedure}", procedureName);

        // Add input parameters
        if (parameters != null && parameters.Count > 0)
        {
            foreach (var param in parameters)
            {
                var paramName = param.Key.TrimStart(':');
                var oracleParam = CreateTypedOracleParameter(paramName, param.Value, ParameterDirection.Input);
                command.Parameters.Add(oracleParam);

                _logger.LogDebug("Added input parameter: {Name} = {Value} (OracleDbType: {DbType})", 
                    paramName, oracleParam.Value, oracleParam.OracleDbType);
            }
        }

        // Add output REF CURSOR parameter
        var cursorParam = new OracleParameter
        {
            ParameterName = "p_cursor",
            OracleDbType = OracleDbType.RefCursor,
            Direction = ParameterDirection.Output
        };
        command.Parameters.Add(cursorParam);

        _logger.LogDebug("Added output cursor parameter: p_cursor");

        // Execute
        await command.ExecuteNonQueryAsync(cancellationToken);

        // Read data from REF CURSOR
        var dataTable = new DataTable();
        
        if (cursorParam.Value is Oracle.ManagedDataAccess.Types.OracleRefCursor refCursor)
        {
            using var reader = refCursor.GetDataReader();
            dataTable.Load(reader);
            
            _logger.LogInformation("Successfully retrieved {RowCount} rows from Oracle procedure {Procedure}", 
                dataTable.Rows.Count, procedureName);
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
        if (parameters != null && parameters.Count > 0)
        {
            foreach (var param in parameters)
            {
                var paramName = param.Key.TrimStart(':');
                var oracleParam = CreateTypedOracleParameter(paramName, param.Value);
                command.Parameters.Add(oracleParam);

                _logger.LogDebug("Added parameter: {Name} = {Value} (OracleDbType: {DbType})", 
                    paramName, oracleParam.Value, oracleParam.OracleDbType);
            }
        }

        // Detect PL/SQL block (contains 'begin', 'end', and 'declare')
        bool isPlSqlBlock =
            sqlQuery.IndexOf("begin", StringComparison.OrdinalIgnoreCase) >= 0 &&
            sqlQuery.IndexOf("end", StringComparison.OrdinalIgnoreCase) >= 0 &&
            sqlQuery.IndexOf("declare", StringComparison.OrdinalIgnoreCase) >= 0;

        var dataTable = new DataTable();

        if (isPlSqlBlock)
        {
            OracleTransaction? transaction = null;
            try
            {
                transaction = connection.BeginTransaction();
                command.Transaction = transaction;
                _logger.LogInformation("PL/SQL block detected. Transaction started for GTT/cursor scope.");

                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                dataTable.Load(reader);

                transaction.Commit();
                _logger.LogInformation("Transaction committed after loading DataTable from PL/SQL block.");
            }
            catch (Exception ex)
            {
                if (transaction != null)
                {
                    try
                    {
                        transaction.Rollback();
                        _logger.LogWarning(ex, "Transaction rolled back due to error in PL/SQL block execution.");
                    }
                    catch (Exception rollbackEx)
                    {
                        _logger.LogError(rollbackEx, "Error during transaction rollback after PL/SQL block failure.");
                    }
                }
                throw;
            }
        }
        else
        {
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            dataTable.Load(reader);
        }

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