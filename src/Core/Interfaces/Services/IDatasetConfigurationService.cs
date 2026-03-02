using DataLakeIngestionService.Core.Models;

namespace DataLakeIngestionService.Core.Interfaces.Services;

public interface IDatasetConfigurationService
{
    Task<List<DatasetConfiguration>> GetDatasetsAsync();
    Task<DatasetConfiguration?> GetDatasetByIdAsync(string datasetId);

    /// <summary>
    /// Returns the physical file path of the dataset-*.json file for the given dataset ID.
    /// </summary>
    Task<string> GetDatasetFilePathAsync(string datasetId);

    /// <summary>
    /// Surgically updates the dataset JSON file with the supplied values and returns the
    /// updated, deserialized configuration. Only fields that are explicitly provided are
    /// changed; all other JSON content is preserved. The in-memory cache is also refreshed.
    /// </summary>
    Task<DatasetConfiguration> UpdateDatasetFileAsync(
        string datasetId,
        string? cronExpression,
        Dictionary<string, string>? parameterUpdates,
        string? uploadProvider,
        CancellationToken ct = default);
}
