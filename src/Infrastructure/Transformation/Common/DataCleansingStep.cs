using System.Data;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Transformation.Common;

public class DataCleansingStep : ITransformationStep
{
    private readonly ILogger<DataCleansingStep> _logger;
    private readonly Dictionary<string, object> _config;

    public DataCleansingStep(
        ILogger<DataCleansingStep> logger,
        Dictionary<string, object>? config = null)
    {
        _logger = logger;
        _config = config ?? new Dictionary<string, object>();
    }

    public string Name => "DataCleansing";
    
    public List<string> Environments { get; set; } = new();

    public Task<DataTable> TransformAsync(DataTable data, CancellationToken cancellationToken)
    {
        var trimWhitespace = GetConfigValue("trimWhitespace", true);
        var removeEmptyStrings = GetConfigValue("removeEmptyStrings", false);
        
        _logger.LogInformation(
            "Applying data cleansing (trimWhitespace={Trim}, removeEmptyStrings={RemoveEmpty})", 
            trimWhitespace, removeEmptyStrings);

        foreach (DataRow row in data.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (DataColumn column in data.Columns)
            {
                if (column.DataType == typeof(string) && row[column] != DBNull.Value)
                {
                    var value = row[column].ToString();

                    if (trimWhitespace && value != null)
                    {
                        value = value.Trim();
                    }

                    if (removeEmptyStrings && string.IsNullOrEmpty(value))
                    {
                        row[column] = DBNull.Value;
                    }
                    else
                    {
                        row[column] = value ?? string.Empty;
                    }
                }
            }
        }

        _logger.LogInformation("Data cleansing completed. Processed {RowCount} rows", data.Rows.Count);

        return Task.FromResult(data);
    }

    private T GetConfigValue<T>(string key, T defaultValue)
    {
        if (!_config.TryGetValue(key, out var value))
            return defaultValue;

        try
        {
            if (value is T typedValue)
                return typedValue;
                
            return (T)Convert.ChangeType(value, typeof(T));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to convert config '{Key}'='{Value}' to {Type}, using default: {Default}", 
                key, value, typeof(T).Name, defaultValue);
            return defaultValue;
        }
    }
}