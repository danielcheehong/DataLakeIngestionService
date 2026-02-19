using System;
using System.Data;

namespace DataLakeIngestionService.Core.Interfaces.ReferenceData;

public interface IReferenceDataProvider
{    
    Task<DataTable> GetAsync(string key, CancellationToken ct);
}
