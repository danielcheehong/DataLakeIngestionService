using System.Text.Json;
using System.Text.Json.Serialization;
using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Core.Models;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Services;

public class DatasetConfigurationService : IDatasetConfigurationService
{
    private readonly ILogger<DatasetConfigurationService> _logger;
    private readonly string _configurationPath;

    public DatasetConfigurationService(
        ILogger<DatasetConfigurationService> logger,
        string configurationPath)
    {
        _logger = logger;
        _configurationPath = configurationPath;
    }

    public async Task<List<DatasetConfiguration>> GetDatasetsAsync()
    {
        var configs = new List<DatasetConfiguration>();

        try
        {
            if (!Directory.Exists(_configurationPath))
            {
                _logger.LogWarning("Dataset configuration directory not found: {Path}", _configurationPath);
                return configs;
            }

            var files = Directory.GetFiles(_configurationPath, "dataset-*.json");

            foreach (var file in files)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    var config = JsonSerializer.Deserialize<DatasetConfiguration>(json, new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true,
                        Converters = {  new JsonStringEnumConverter() } // To handle data source type enums as strings.
                    });

                    if (config != null)
                    {
                            // Convert JsonElement parameters to native types
                        if (config.Source?.Parameters != null)
                        {
                            config.Source.Parameters = ConvertJsonElementParameters(config.Source.Parameters);
                        }
                        configs.Add(config);
                        _logger.LogInformation("Loaded dataset configuration: {DatasetId}", config.DatasetId);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to load dataset configuration from file: {File}", file);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load dataset configurations");
        }

        return configs;
    }

    public async Task<DatasetConfiguration?> GetDatasetByIdAsync(string datasetId)
    {
        var datasets = await GetDatasetsAsync();
        return datasets.FirstOrDefault(d => d.DatasetId == datasetId);
    }
    
     /// <summary>
    /// Converts JsonElement values in parameter dictionary to native .NET types
    /// </summary>
    private Dictionary<string, object> ConvertJsonElementParameters(Dictionary<string, object> parameters)
    {
        var converted = new Dictionary<string, object>();

        foreach (var kvp in parameters)
        {
            if (kvp.Value is JsonElement jsonElement)
            {
                converted[kvp.Key] = ConvertJsonElement(jsonElement);
            }
            else
            {
                converted[kvp.Key] = kvp.Value;
            }
        }

        return converted;
    }


    /// <summary>
    /// Converts a JsonElement to its appropriate .NET type
    /// </summary>
    private object ConvertJsonElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString() ?? string.Empty,
            JsonValueKind.Number => element.TryGetInt32(out var intValue) ? intValue :
                                   element.TryGetInt64(out var longValue) ? longValue :
                                   element.TryGetDecimal(out var decimalValue) ? decimalValue :
                                   element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => DBNull.Value,
            JsonValueKind.Array => element.EnumerateArray()
                                         .Select(ConvertJsonElement)
                                         .ToList(),
            JsonValueKind.Object => element.EnumerateObject()
                                          .ToDictionary(p => p.Name, p => ConvertJsonElement(p.Value)),
            _ => element.GetRawText()
        };
    }
}
