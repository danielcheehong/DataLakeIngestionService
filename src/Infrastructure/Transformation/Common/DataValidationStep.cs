using System.Data;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Transformation.Common;

public class DataValidationStep : ITransformationStep
{
    private readonly ILogger<DataValidationStep> _logger;
    private readonly Dictionary<string, object> _config;

    public DataValidationStep(
        ILogger<DataValidationStep> logger,
        Dictionary<string, object>? config = null)
    {
        _logger = logger;
        _config = config ?? new Dictionary<string, object>();
    }

    public string Name => "DataValidation";
    
    public List<string> Environments { get; set; } = new();

    public Task TransformAsync(IPipelineContext context, CancellationToken cancellationToken)
    {
        var requiredColumns = GetConfigValue<string[]>("requiredColumns", Array.Empty<string>());
        var validateEmail = GetConfigValue("validateEmail", false);
        
        _logger.LogInformation("Applying data validation");

        var totalRows = 0;

        // Validate ExtractedData if available
        if (context.ExtractedData != null)
        {
            ValidateDataTable(context.ExtractedData, requiredColumns, "ExtractedData");
            totalRows += context.ExtractedData.Rows.Count;
        }

        // Validate each table in ExtractedDataSets
        foreach (var kvp in context.ExtractedDataSets)
        {
            ValidateDataTable(kvp.Value, requiredColumns, kvp.Key);
            totalRows += kvp.Value.Rows.Count;
        }

        _logger.LogInformation("Data validation completed. {RowCount} total rows validated", totalRows);

        return Task.CompletedTask;
    }

    private void ValidateDataTable(DataTable data, string[] requiredColumns, string sourceName)
    {
        // Validate required columns exist
        foreach (var columnName in requiredColumns)
        {
            if (!data.Columns.Contains(columnName))
            {
                throw new InvalidOperationException(
                    $"Required column '{columnName}' not found in dataset '{sourceName}'");
            }
        }

        _logger.LogDebug("Validated {RowCount} rows in '{SourceName}'", data.Rows.Count, sourceName);
    }

    private T GetConfigValue<T>(string key, T defaultValue)
    {
        if (!_config.TryGetValue(key, out var value))
            return defaultValue;

        try
        {
            if (value is T typedValue)
                return typedValue;
                
            if (value is System.Text.Json.JsonElement jsonElement)
            {
                // Handle JSON deserialization for arrays/objects
                return System.Text.Json.JsonSerializer.Deserialize<T>(jsonElement.GetRawText())
                    ?? defaultValue;
            }
                
            return (T)Convert.ChangeType(value, typeof(T));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to convert config '{Key}' to {Type}, using default", 
                key, typeof(T).Name);
            return defaultValue;
        }
    }
}
