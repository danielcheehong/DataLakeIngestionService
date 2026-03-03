using DataLakeIngestionService.Core.Security;

namespace DataLakeIngestionService.Core.Interfaces.Vault;

public interface IVaultService
{
    /// <summary>
    /// Retrieves a secret from the vault.
    /// </summary>
    /// <param name="secretPath">Path to the secret (e.g., "oracle/hr_password")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>
    /// The secret value wrapped in a <see cref="SecretValue"/> whose internal buffer
    /// can be zeroed by calling <see cref="SecretValue.Dispose"/>. Callers should dispose
    /// the returned value as soon as it is no longer needed.
    /// </returns>
    Task<SecretValue> GetSecretAsync(string secretPath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the vault provider name.
    /// </summary>
    string ProviderName { get; }
}
