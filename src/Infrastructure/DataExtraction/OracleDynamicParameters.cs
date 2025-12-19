using Dapper;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.Text.Json;

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
        // Convert JsonElement to native type first
        var convertedValue = ConvertJsonElement(value);

        var param = new OracleParameter
        {
            ParameterName = name,
            Direction = direction ?? ParameterDirection.Input
        };

        // Handle null values
        if (convertedValue == null)
        {
            param.Value = DBNull.Value;
        }
        // If dbType is explicitly specified, use it
        else if (dbType.HasValue)
        {
            param.OracleDbType = dbType.Value;
            param.Value = convertedValue;
        }
        // Otherwise, infer type from value and convert if needed
        else
        {
            SetOracleParameterValue(param, convertedValue);
        }

        if (size.HasValue)
            param.Size = size.Value;

        _oracleParameters.Add(param);
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

    /// <summary>
    /// Sets the Oracle parameter value with automatic type conversion
    /// </summary>
    private static void SetOracleParameterValue(OracleParameter param, object value)
    {
        // Handle string values from JSON configuration
        if (value is string strValue)
        {
            // Try to parse as number
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
            else if (DateTime.TryParse(strValue, out DateTime dateValue))
            {
                param.OracleDbType = OracleDbType.Date;
                param.Value = dateValue;
            }
            else if (bool.TryParse(strValue, out bool boolValue))
            {
                param.OracleDbType = OracleDbType.Byte;
                param.Value = boolValue ? 1 : 0;
            }
            else
            {
                // Default to VARCHAR2 for strings
                param.OracleDbType = OracleDbType.Varchar2;
                param.Value = strValue;
            }
        }
        // Handle native .NET types
        else
        {
            param.OracleDbType = value switch
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
            
            param.Value = value is bool bValue ? (bValue ? 1 : 0) : value;
        }
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