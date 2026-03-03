using System;
using System.Text.RegularExpressions;
using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Core.Interfaces.Vault;
using DataLakeIngestionService.Core.Security;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Services;

public class ConnectionStringBuilder : IConnectionStringBuilder
{
    private readonly IVaultService _vaultService;
    private readonly IMemoryCache _cache;
    private readonly ILogger<ConnectionStringBuilder> _logger;

    // Regex to match: {vault:path/to/secret}
    private static readonly Regex VaultPlaceholderRegex =
        new Regex(@"\{vault:([^}]+)\}", RegexOptions.Compiled);

    public ConnectionStringBuilder(
        IVaultService vaultService,
        IMemoryCache cache,
        ILogger<ConnectionStringBuilder> logger)
    {
        _vaultService = vaultService;
        _cache = cache;
        _logger = logger;
    }

    /// <inheritdoc/>
    public async Task<SecretValue> BuildConnectionStringAsync(
        string connectionStringTemplate,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionStringTemplate))
        {
            throw new ArgumentException("Connection string template cannot be empty",
                nameof(connectionStringTemplate));
        }

        var matches = VaultPlaceholderRegex.Matches(connectionStringTemplate);

        if (matches.Count == 0)
        {
            _logger.LogDebug("No vault placeholders found in connection string");
            // No secrets involved — wrap the plain template directly.
            return new SecretValue(connectionStringTemplate);
        }

        _logger.LogInformation("Found {Count} vault placeholders to resolve", matches.Count);

        // Holds per-match secret fragments as char[]. Each entry is a clone owned by this
        // method and must be zeroed in the finally block regardless of outcome.
        var secretFragments = new char[matches.Count][];
        // The assembled connection string buffer. Set to null once ownership is transferred
        // to the returned SecretValue so the finally block does not zero it prematurely.
        char[]? assemblyBuffer = null;

        try
        {
            // ── Phase 1: resolve all placeholders upfront ─────────────────────────────
            for (var i = 0; i < matches.Count; i++)
            {
                var secretPath = matches[i].Groups[1].Value;
                secretFragments[i] = await GetSecretWithCacheAsync(secretPath, cancellationToken);
                _logger.LogDebug("Resolved vault placeholder: {Path}", secretPath);
            }

            // ── Phase 2: calculate exact output length ─────────────────────────────────
            var totalLength = connectionStringTemplate.Length;
            for (var i = 0; i < matches.Count; i++)
                totalLength += secretFragments[i].Length - matches[i].Length;

            // ── Phase 3: splice template + secrets into a single char[] ────────────────
            assemblyBuffer = new char[totalLength];
            var templateSpan = connectionStringTemplate.AsSpan();
            var srcPos = 0;
            var destPos = 0;

            for (var i = 0; i < matches.Count; i++)
            {
                // Copy the template segment that precedes this placeholder.
                var segLen = matches[i].Index - srcPos;
                templateSpan.Slice(srcPos, segLen).CopyTo(assemblyBuffer.AsSpan(destPos));
                destPos += segLen;
                srcPos = matches[i].Index + matches[i].Length;

                // Splice the secret characters in place of the placeholder.
                secretFragments[i].AsSpan().CopyTo(assemblyBuffer.AsSpan(destPos));
                destPos += secretFragments[i].Length;
            }

            // Copy any trailing template characters after the last placeholder.
            templateSpan.Slice(srcPos).CopyTo(assemblyBuffer.AsSpan(destPos));

            _logger.LogInformation("Successfully resolved all vault placeholders");

            // Transfer ownership of assemblyBuffer to SecretValue.
            // Setting assemblyBuffer = null prevents the finally block from zeroing it;
            // SecretValue.Dispose() will zero it when the caller is done.
            var result = new SecretValue(assemblyBuffer);
            assemblyBuffer = null;
            return result;
        }
        finally
        {
            // Always zero every per-match secret fragment to minimise heap exposure.
            foreach (var fragment in secretFragments)
            {
                if (fragment is not null)
                    Array.Clear(fragment, 0, fragment.Length);
            }

            // Zero the assembly buffer only on the exception path (non-null means
            // ownership was NOT transferred, i.e. we are unwinding due to an error).
            if (assemblyBuffer is not null)
                Array.Clear(assemblyBuffer, 0, assemblyBuffer.Length);
        }
    }

    /// <inheritdoc/>
    public bool ContainsVaultPlaceholders(string connectionString)
    {
        return !string.IsNullOrWhiteSpace(connectionString)
               && VaultPlaceholderRegex.IsMatch(connectionString);
    }

    /// <summary>
    /// Returns an independent copy of the secret characters for <paramref name="secretPath"/>.
    ///
    /// Cache storage: <c>char[]</c> (NOT <c>string</c>) with a <see cref="MemoryCacheEntryOptions"/>
    /// post-eviction callback that calls <see cref="Array.Clear"/> on the cached array,
    /// zeroing the sensitive bytes as soon as the entry leaves the cache.
    ///
    /// The method always returns a *clone* of the cached array so that the caller's
    /// subsequent <see cref="Array.Clear"/> (in <see cref="BuildConnectionStringAsync"/>'s
    /// finally block) does not corrupt the cache entry.
    /// </summary>
    private async Task<char[]> GetSecretWithCacheAsync(
        string secretPath,
        CancellationToken cancellationToken)
    {
        var cacheKey = $"vault_secret_{secretPath}";

        // ── Cache hit: return a copy so callers can zero their copy independently ─────
        if (_cache.TryGetValue<char[]>(cacheKey, out var cachedChars))
        {
            _logger.LogDebug("Retrieved secret from cache: {Path}", secretPath);
            return (char[])cachedChars!.Clone();
        }

        // ── Cache miss: fetch from vault ───────────────────────────────────────────────
        _logger.LogInformation("Retrieving secret from vault: {Path}", secretPath);

        using var secretValue = await _vaultService.GetSecretAsync(secretPath, cancellationToken);

        // Copy the secret into a char[] that will be stored in the cache.
        var charsForCache = secretValue.CopyBuffer();

        // secretValue is disposed here (end of using block), zeroing its buffer.

        var cacheOptions = new MemoryCacheEntryOptions()
            .SetAbsoluteExpiration(TimeSpan.FromMinutes(5))
            .SetPriority(CacheItemPriority.High)
            // Zero the cached char[] as soon as the entry is evicted from memory.
            .RegisterPostEvictionCallback(static (_, value, _, _) =>
            {
                if (value is char[] chars)
                    Array.Clear(chars, 0, chars.Length);
            });

        _cache.Set(cacheKey, charsForCache, cacheOptions);

        // Return an independent copy for this call; the cache retains its own copy.
        return (char[])charsForCache.Clone();
    }
}
