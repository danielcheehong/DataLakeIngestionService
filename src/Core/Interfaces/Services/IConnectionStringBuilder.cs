using DataLakeIngestionService.Core.Security;

namespace DataLakeIngestionService.Core.Interfaces.Services;

public interface IConnectionStringBuilder
{
    /// <summary>
    /// Builds a connection string by resolving all vault placeholders.
    /// The returned <see cref="SecretValue"/> wraps the assembled connection string in a
    /// zeroable <c>char[]</c> buffer. Callers must dispose the value as soon as the
    /// consuming statement (e.g. a connection constructor) has been executed.
    /// </summary>
    /// <param name="connectionStringTemplate">Connection string with vault placeholders.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Assembled connection string with resolved secrets wrapped in a <see cref="SecretValue"/>.</returns>
    Task<SecretValue> BuildConnectionStringAsync(
        string connectionStringTemplate,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> when the connection string contains one or more vault placeholders.
    /// </summary>
    bool ContainsVaultPlaceholders(string connectionString);
}
