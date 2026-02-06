using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.DataExtraction;

public class DataSourceFactory : IDataSourceFactory
{
    private readonly ILogger<SqlServerDataSource> _sqlServerLogger;
    private readonly ILogger<OracleDataSource> _oracleLogger;
    private readonly ILogger<DotNetDataSource> _dotNetLogger;
    private readonly IDotNetDataProviderFactory _dotNetProviderFactory;

    public DataSourceFactory(
        ILogger<SqlServerDataSource> sqlServerLogger,
        ILogger<OracleDataSource> oracleLogger,
        ILogger<DotNetDataSource> dotNetLogger,
        IDotNetDataProviderFactory dotNetProviderFactory)
    {
        _sqlServerLogger = sqlServerLogger;
        _oracleLogger = oracleLogger;
        _dotNetLogger = dotNetLogger;
        _dotNetProviderFactory = dotNetProviderFactory;
    }

    public IDataSource Create(string sourceType)
    {
        return sourceType.ToLowerInvariant() switch
        {
            "sqlserver" => new SqlServerDataSource(_sqlServerLogger),
            "oracle" => new OracleDataSource(_oracleLogger),
            "dotnet" => new DotNetDataSource(_dotNetLogger, _dotNetProviderFactory),
            _ => throw new ArgumentException($"Unsupported source type: {sourceType}", nameof(sourceType))
        };
    }
}
