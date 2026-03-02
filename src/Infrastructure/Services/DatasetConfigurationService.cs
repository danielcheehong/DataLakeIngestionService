using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Core.Models;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Services;

public class DatasetConfigurationService : IDatasetConfigurationService
{
    private readonly ILogger<DatasetConfigurationService> _logger;
    private readonly string _configurationPath;

    // Cache stores both the deserialized config and its physical file path
    private Dictionary<string, (DatasetConfiguration Config, string FilePath)> _cache = new();

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public DatasetConfigurationService(
        ILogger<DatasetConfigurationService> logger,
        string configurationPath)
    {
        _logger = logger;
        _configurationPath = configurationPath;
    }

    public async Task<List<DatasetConfiguration>> GetDatasetsAsync()
    {
        // Return from cache if already loaded
        if (_cache.Count > 0)
            return _cache.Values.Select(x => x.Config).ToList();

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
                    var config = JsonSerializer.Deserialize<DatasetConfiguration>(json, _jsonOptions);

                    // normalize parameters/config dictionaries so they are not JsonElement
                    if (config != null)
                    {
                        NormalizeConfig(config);
                        _cache[config.DatasetId] = (config, file);
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
        if (_cache.Count == 0)
            await GetDatasetsAsync();

        return _cache.TryGetValue(datasetId, out var tuple) ? tuple.Config : null;
    }

    public async Task<string> GetDatasetFilePathAsync(string datasetId)
    {
        if (_cache.Count == 0)
            await GetDatasetsAsync();

        if (!_cache.TryGetValue(datasetId, out var tuple))
            throw new KeyNotFoundException($"Dataset '{datasetId}' not found.");

        return tuple.FilePath;
    }

    public async Task<DatasetConfiguration> UpdateDatasetFileAsync(
        string datasetId,
        string? cronExpression,
        Dictionary<string, string>? parameterUpdates,
        string? uploadProvider,
        CancellationToken ct = default)
    {
        var filePath = await GetDatasetFilePathAsync(datasetId);

        var json = await File.ReadAllTextAsync(filePath, ct);
        var root = JsonNode.Parse(json)
            ?? throw new InvalidOperationException($"Failed to parse JSON for dataset '{datasetId}'.");

        ApplyUpdatesToNode(root, cronExpression, parameterUpdates, uploadProvider);

        var writeOptions = new JsonSerializerOptions { WriteIndented = true };
        var updatedJson = root.ToJsonString(writeOptions);
        await File.WriteAllTextAsync(filePath, updatedJson, ct);

        var updatedConfig = JsonSerializer.Deserialize<DatasetConfiguration>(updatedJson, _jsonOptions)
            ?? throw new InvalidOperationException($"Failed to deserialize updated config for dataset '{datasetId}'.");

        NormalizeConfig(updatedConfig);
        _cache[datasetId] = (updatedConfig, filePath);

        _logger.LogInformation(
            "Updated dataset config '{DatasetId}': cron={Cron}, provider={Provider}, paramCount={ParamCount}",
            datasetId, cronExpression ?? "-", uploadProvider ?? "-", parameterUpdates?.Count ?? 0);

        return updatedConfig;
    }
    
    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Applies the supplied updates surgically to a JsonNode tree.
    /// Only keys/paths that are explicitly provided are touched.
    /// </summary>
    private static void ApplyUpdatesToNode(
        JsonNode root,
        string? cronExpression,
        Dictionary<string, string>? parameterUpdates,
        string? uploadProvider)
    {
        if (!string.IsNullOrWhiteSpace(cronExpression))
            root["cronExpression"] = cronExpression;

        if (!string.IsNullOrWhiteSpace(uploadProvider))
        {
            root["upload"] ??= new JsonObject();
            root["upload"]!["provider"] = uploadProvider;
        }

        if (parameterUpdates != null && parameterUpdates.Count > 0)
        {
            // Single source
            ApplyParameterUpdatesToSourceNode(root["source"], parameterUpdates);

            // Multi-source
            if (root["sources"] is JsonArray sources)
            {
                foreach (var source in sources)
                    ApplyParameterUpdatesToSourceNode(source, parameterUpdates);
            }
        }
    }

    /// <summary>
    /// Updates matching parameter keys inside a source node's "parameters" object.
    /// Only keys that already exist in the JSON are updated; no new keys are injected.
    /// </summary>
    private static void ApplyParameterUpdatesToSourceNode(
        JsonNode? sourceNode,
        Dictionary<string, string> updates)
    {
        if (sourceNode?["parameters"] is not JsonObject parameters)
            return;

        foreach (var (key, value) in updates)
        {
            if (parameters.ContainsKey(key))
                parameters[key] = value;
        }
    }

    /// <summary>
    /// Runs all post-deserialization normalization on a freshly deserialized config
    /// (converts JsonElement parameter values to native .NET types).
    /// </summary>
    private void NormalizeConfig(DatasetConfiguration config)
    {
        if (config.Source?.Parameters != null)
            config.Source.Parameters = ConvertJsonElementParameters(config.Source.Parameters);

        if (config.Sources != null)
        {
            foreach (var src in config.Sources)
            {
                if (src?.Parameters != null)
                    src.Parameters = ConvertJsonElementParameters(src.Parameters);
            }
        }

        if (config.Transformations != null)
        {
            foreach (var t in config.Transformations)
            {
                if (t?.Config != null)
                    t.Config = ConvertJsonElementParameters(t.Config);
            }
        }
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
