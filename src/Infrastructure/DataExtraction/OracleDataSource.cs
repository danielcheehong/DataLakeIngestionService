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

            // Add input parameters
            if (parameters != null)
            {
                foreach (var param in parameters)
                {
                    dynamicParams.Add(param.Key, param.Value);
                }
            }

            // Add REF CURSOR for result set (Oracle standard pattern)
            dynamicParams.Add("p_cursor", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

            _logger.LogInformation("Executing Oracle package: {Query}", query);

            await connection.ExecuteAsync(
                query,
                dynamicParams,
                commandType: CommandType.StoredProcedure,
                commandTimeout: 600);

            // Retrieve REF CURSOR
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
