using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.DataExtraction;

public class DataSourceFactory : IDataSourceFactory
{
    private readonly ILogger<SqlServerDataSource> _sqlServerLogger;
    private readonly ILogger<OracleDataSource> _oracleLogger;

    public DataSourceFactory(
        ILogger<SqlServerDataSource> sqlServerLogger,
        ILogger<OracleDataSource> oracleLogger)
    {
        _sqlServerLogger = sqlServerLogger;
        _oracleLogger = oracleLogger;
    }

    public IDataSource Create(string sourceType)
    {
        return sourceType.ToLowerInvariant() switch
        {
            "sqlserver" => new SqlServerDataSource(_sqlServerLogger),
            "oracle" => new OracleDataSource(_oracleLogger),
            _ => throw new ArgumentException($"Unsupported source type: {sourceType}", nameof(sourceType))
        };
    }
}
