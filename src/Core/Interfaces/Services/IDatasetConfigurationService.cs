using DataLakeIngestionService.Core.Models;

namespace DataLakeIngestionService.Core.Interfaces.Services;

public interface IDatasetConfigurationService
{
    Task<List<DatasetConfiguration>> GetDatasetsAsync();
    Task<DatasetConfiguration?> GetDatasetByIdAsync(string datasetId);
}
