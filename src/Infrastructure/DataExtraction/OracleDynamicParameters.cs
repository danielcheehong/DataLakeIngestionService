using Dapper;
using Oracle.ManagedDataAccess.Client;
using System.Data;

namespace DataLakeIngestionService.Infrastructure.DataExtraction;

/// <summary>
/// Custom DynamicParameters implementation for Oracle-specific parameter handling
/// </summary>
public class OracleDynamicParameters : SqlMapper.IDynamicParameters
{
    private readonly DynamicParameters _dynamicParameters = new();
    private readonly List<OracleParameter> _oracleParameters = new();

    public void Add(string name, object? value = null, OracleDbType? dbType = null,
        ParameterDirection? direction = null, int? size = null)
    {
        var param = new OracleParameter
        {
            ParameterName = name,
            Value = value ?? DBNull.Value,
            Direction = direction ?? ParameterDirection.Input
        };

        if (dbType.HasValue)
            param.OracleDbType = dbType.Value;

        if (size.HasValue)
            param.Size = size.Value;

        _oracleParameters.Add(param);
    }

    public void AddDynamicParams(object param)
    {
        _dynamicParameters.AddDynamicParams(param);
    }

    public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
    {
        if (command is OracleCommand oracleCommand)
        {
            foreach (var param in _oracleParameters)
            {
                oracleCommand.Parameters.Add(param);
            }
        }

        //_dynamicParameters.AddParameters(command, identity);
        // Cast to interface to access the protected method
        ((SqlMapper.IDynamicParameters)_dynamicParameters).AddParameters(command, identity);
    }

    public T Get<T>(string name)
    {
        var param = _oracleParameters.FirstOrDefault(p => p.ParameterName == name);
        if (param != null)
        {
          
            return (T)Convert.ChangeType(param.Value, typeof(T));
        }

        return _dynamicParameters.Get<T>(name);
    }
        /// <summary>
    /// Gets the OracleParameter by name for accessing REF CURSOR data readers
    /// </summary>
    public OracleParameter? GetOracleParameter(string name)
    {
        return _oracleParameters.FirstOrDefault(p => p.ParameterName == name);
    }
}
