using System.Text.Json;
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
        var datasets = new List<DatasetConfiguration>();

        try
        {
            if (!Directory.Exists(_configurationPath))
            {
                _logger.LogWarning("Dataset configuration directory not found: {Path}", _configurationPath);
                return datasets;
            }

            var files = Directory.GetFiles(_configurationPath, "dataset-*.json");

            foreach (var file in files)
            {
                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    var dataset = JsonSerializer.Deserialize<DatasetConfiguration>(json, new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                    if (dataset != null)
                    {
                        datasets.Add(dataset);
                        _logger.LogInformation("Loaded dataset configuration: {DatasetId}", dataset.DatasetId);
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

        return datasets;
    }

    public async Task<DatasetConfiguration?> GetDatasetByIdAsync(string datasetId)
    {
        var datasets = await GetDatasetsAsync();
        return datasets.FirstOrDefault(d => d.DatasetId == datasetId);
    }
}
