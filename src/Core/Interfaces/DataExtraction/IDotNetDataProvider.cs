using System.Data;

namespace DataLakeIngestionService.Core.Interfaces.DataExtraction;

/// <summary>
/// Interface for C# code-based data providers that generate DataTables programmatically.
/// Implement this interface to create custom data generators that can be used as a data source
/// in the ingestion pipeline without requiring a database connection.
/// </summary>
public interface IDotNetDataProvider
{
    /// <summary>
    /// Unique name to identify this provider in dataset configuration.
    /// This name is used in the dataset JSON configuration to specify which provider to invoke.
    /// </summary>
    string ProviderName { get; }

    /// <summary>
    /// Generates data as a DataTable using C# logic.
    /// </summary>
    /// <param name="parameters">Optional parameters passed from dataset configuration.
    /// Parameter values are resolved (e.g., ${today} becomes actual date) before being passed.</param>
    /// <param name="cancellationToken">Cancellation token for async operations.</param>
    /// <returns>A DataTable containing the generated data.</returns>
    Task<DataTable> GenerateDataAsync(
        Dictionary<string, object>? parameters,
        CancellationToken cancellationToken);
}

/// <summary>
/// Factory for creating IDotNetDataProvider instances by name.
/// Discovers providers via assembly scanning at startup.
/// </summary>
public interface IDotNetDataProviderFactory
{
    /// <summary>
    /// Creates a provider instance by name.
    /// </summary>
    /// <param name="providerName">The provider name (derived from class name by removing "Provider" suffix).</param>
    /// <returns>The provider instance.</returns>
    IDotNetDataProvider Create(string providerName);

    /// <summary>
    /// Gets a list of all available provider names.
    /// </summary>
    IEnumerable<string> GetAvailableProviders();
}
