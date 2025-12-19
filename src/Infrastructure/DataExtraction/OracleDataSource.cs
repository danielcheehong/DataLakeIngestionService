using System.Data;
using Dapper;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;

namespace DataLakeIngestionService.Infrastructure.DataExtraction;

public class OracleDataSource : IDataSource
{
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

            var dynamicParams = new OracleDynamicParameters();

            // Build parameter list for PL/SQL block
            var paramList = new List<string>();

            // Add input parameters
            if (parameters != null)
            {
                foreach (var param in parameters)
                {
                    // Remove colon prefix if present in parameter name
                    var paramName = param.Key.TrimStart(':');
                    
                    _logger.LogDebug("Adding parameter {Name} = {Value} (Type: {Type})", 
                        paramName, param.Value, param.Value?.GetType().Name ?? "null");
                    
                    // Add parameter without colon
                    dynamicParams.Add(paramName, param.Value);
                    
                    // Add to parameter list WITH colon for PL/SQL block
                    paramList.Add($":{paramName}");
                }
            }

            // Add REF CURSOR for result set (without colon in parameter name)
            dynamicParams.Add("p_cursor", 
                dbType: OracleDbType.RefCursor, 
                direction: ParameterDirection.Output);
            
            // Add cursor to parameter list WITH colon for PL/SQL block
            paramList.Add(":p_cursor");

            // Determine if this is a package call
            CommandType commandType;
            string executionQuery;
            
            if (query.Contains("."))
            {
                // Package call - use PL/SQL anonymous block
                commandType = CommandType.Text;
                executionQuery = $"BEGIN {query}({string.Join(", ", paramList)}); END;";
                
                _logger.LogInformation("Executing Oracle package with PL/SQL block: {SqlCommand}", executionQuery);
            }
            else
            {
                // Standalone stored procedure
                commandType = CommandType.StoredProcedure;
                executionQuery = query;
                
                _logger.LogInformation("Executing Oracle stored procedure: {Query}", query);
            }

            // Execute the command
            await connection.ExecuteAsync(
                executionQuery,
                dynamicParams,
                commandType: commandType,
                commandTimeout: 600);

            // Retrieve REF CURSOR (without colon)
            var refCursorParam = dynamicParams.GetOracleParameter("p_cursor");

            if (refCursorParam?.Value == null || refCursorParam.Value == DBNull.Value)
            {
                throw new ExtractionException("REF CURSOR parameter was not populated by the stored procedure");
            }

            var dataTable = new DataTable();
            
            // Cast the parameter value to OracleRefCursor and get the reader
            if (refCursorParam.Value is Oracle.ManagedDataAccess.Types.OracleRefCursor refCursor)
            {
                using var reader = refCursor.GetDataReader();
                dataTable.Load(reader);
            }
            else
            {
                throw new ExtractionException(
                    $"Expected OracleRefCursor but got {refCursorParam.Value.GetType().Name}");
            }

            _logger.LogInformation("Retrieved {RowCount} rows from Oracle", dataTable.Rows.Count);

            return dataTable;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to extract data from Oracle");
            throw new ExtractionException($"Oracle extraction failed: {ex.Message}", ex);
        }
    }
}