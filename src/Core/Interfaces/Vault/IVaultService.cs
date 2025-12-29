using System;

namespace DataLakeIngestionService.Core.Interfaces.Vault;

public interface IVaultService
{
    /// <summary>
    /// Retrieves a secret from the vault
    /// </summary>
    /// <param name="secretPath">Path to the secret (e.g., "oracle/hr_password")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The secret value</returns>
    Task<string> GetSecretAsync(string secretPath, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the vault provider name
    /// </summary>
    string ProviderName { get; }
}
