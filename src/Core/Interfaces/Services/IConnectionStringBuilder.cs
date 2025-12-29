namespace DataLakeIngestionService.Core.Interfaces.Services;

public interface IConnectionStringBuilder
{
    /// <summary>
    /// Builds a connection string by resolving vault placeholders
    /// </summary>
    /// <param name="connectionStringTemplate">Connection string with vault placeholders</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Connection string with resolved passwords</returns>
    Task<string> BuildConnectionStringAsync(
        string connectionStringTemplate, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Validates if a connection string contains vault placeholders
    /// </summary>
    bool ContainsVaultPlaceholders(string connectionString);
}
