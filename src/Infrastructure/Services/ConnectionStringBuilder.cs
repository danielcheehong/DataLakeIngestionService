using System;
using System.Text.RegularExpressions;
using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Core.Interfaces.Vault;
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

    public async Task<string> BuildConnectionStringAsync(
        string connectionStringTemplate, 
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionStringTemplate))
        {
            throw new ArgumentException("Connection string template cannot be empty", 
                nameof(connectionStringTemplate));
        }

        // Find all vault placeholders
        var matches = VaultPlaceholderRegex.Matches(connectionStringTemplate);
        
        if (matches.Count == 0)
        {
            _logger.LogDebug("No vault placeholders found in connection string");
            return connectionStringTemplate; // No placeholders to replace, just return as is.
        }

        _logger.LogInformation("Found {Count} vault placeholders to resolve", matches.Count);

        var resolvedConnectionString = connectionStringTemplate;

        // Replace each placeholder with actual secret from vault
        foreach (Match match in matches)
        {
            var fullPlaceholder = match.Value; // {vault:oracle/hr_password}
            var secretPath = match.Groups[1].Value; // oracle/hr_password

            var secret = await GetSecretWithCacheAsync(secretPath, cancellationToken);

            resolvedConnectionString = resolvedConnectionString.Replace(fullPlaceholder, secret);
            
            _logger.LogDebug("Resolved vault placeholder: {Path}", secretPath);
        }

        _logger.LogInformation("Successfully resolved all vault placeholders");
        
        return resolvedConnectionString;
    }

    public bool ContainsVaultPlaceholders(string connectionString)
    {
        return !string.IsNullOrWhiteSpace(connectionString) 
               && VaultPlaceholderRegex.IsMatch(connectionString);
    }

    private async Task<string> GetSecretWithCacheAsync(
        string secretPath, 
        CancellationToken cancellationToken)
    {
        var cacheKey = $"vault_secret_{secretPath}";

        // Try to get from cache first
        if (_cache.TryGetValue<string>(cacheKey, out var cachedSecret))
        {
            _logger.LogDebug("Retrieved secret from cache: {Path}", secretPath);
            return cachedSecret!;
        }

        // Retrieve from vault
        _logger.LogInformation("Retrieving secret from vault: {Path}", secretPath);
        
        var secret = await _vaultService.GetSecretAsync(secretPath, cancellationToken);

        // Cache for 5 minutes
        var cacheOptions = new MemoryCacheEntryOptions()
            .SetAbsoluteExpiration(TimeSpan.FromMinutes(5))
            .SetPriority(CacheItemPriority.High);

        _cache.Set(cacheKey, secret, cacheOptions);

        return secret;
    }
}
