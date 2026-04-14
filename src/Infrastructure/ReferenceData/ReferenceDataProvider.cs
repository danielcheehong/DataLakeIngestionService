using System.Collections.Concurrent;
using System.Data;
using DataLakeIngestionService.Core.Enums;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using DataLakeIngestionService.Core.Interfaces.ReferenceData;
using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Core.Security;
using DataLakeIngestionService.Infrastructure.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.ReferenceData;

public class ReferenceDataProvider: IReferenceDataProvider
{
    private readonly ILogger<ReferenceDataProvider> _logger;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _configuration;
    private readonly IHostEnvironment _env;

    private sealed record ReferenceDefinition(
        string SourceType,               // "Oracle" | "SqlServer"
        string ConnectionStringName,      // e.g. "HROracleDB"
        string QueryOrCommand,            // SQL text or proc name
        ExtractionType ExtractionType,    // Query / StoredProcedure / Package
        TimeSpan Ttl,
        Dictionary<string, object>? Parameters = null);

    private sealed record CacheEntry(DataTable Master, DateTime ExpiresUtc);

    // cacheKey -> lazy async load
    private readonly ConcurrentDictionary<string, Lazy<Task<CacheEntry>>> _cache = new();

    // Central registry for reference datasets
    private readonly IReadOnlyDictionary<string, ReferenceDefinition> _defs;

    public ReferenceDataProvider(
        ILogger<ReferenceDataProvider> logger,
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        IHostEnvironment env)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _configuration = configuration;
        _env = env;

        // Register all reference data keys here (simple, explicit).
        _defs = new Dictionary<string, ReferenceDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            // Example: whitelist view in Oracle with same name as CSV
            ["IwmWhitelist"] = new ReferenceDefinition(
                SourceType: "Oracle",
                ConnectionStringName: "HROracleDB",
                QueryOrCommand: "SELECT * FROM VFOC_FILTRA_CONTA_SCV_IWM_UBS",
                ExtractionType: ExtractionType.Query,
                Ttl: TimeSpan.FromMinutes(10)),

            ["ReferenceDate"] = new ReferenceDefinition(
                SourceType: "Oracle",
                ConnectionStringName: "HROracleDB",
                QueryOrCommand: "YOUR_PROC_NAME",
                ExtractionType: ExtractionType.StoredProcedure,
                Ttl: TimeSpan.FromHours(1))
        };
    }

    public async Task<DataTable> GetAsync(string key, CancellationToken ct)
    {
        if (!_defs.TryGetValue(key, out var def))
            throw new KeyNotFoundException($"Reference data key '{key}' is not registered.");

        // env-scope prevents mixing Dev/Staging/Prod values in same process
        var cacheKey = $"{_env.EnvironmentName}::{key}";

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var lazy = _cache.GetOrAdd(cacheKey, _ =>
                new Lazy<Task<CacheEntry>>(() => LoadAndCacheAsync(key, def, CancellationToken.None)));

            CacheEntry entry;
            try
            {
                entry = await lazy.Value;
            }
            catch
            {
                // failed load should not poison cache — remove by identity to avoid
                // evicting a healthy Lazy that a concurrent caller already inserted
                _cache.TryRemove(new KeyValuePair<string, Lazy<Task<CacheEntry>>>(cacheKey, lazy));
                throw;
            }

            if (DateTime.UtcNow <= entry.ExpiresUtc)
            {
                // Return a copy so callers can't mutate the cached master
                return entry.Master.Copy();
            }

            // expired: remove and loop to reload — remove by identity only
            _cache.TryRemove(new KeyValuePair<string, Lazy<Task<CacheEntry>>>(cacheKey, lazy));
        }
    }

    private async Task<CacheEntry> LoadAndCacheAsync(string key, ReferenceDefinition def, CancellationToken ct)
    {
        var connectionStringTemplate = _configuration.GetConnectionString(def.ConnectionStringName);

        using var scope = _scopeFactory.CreateScope();
        var dataSourceFactory = scope.ServiceProvider.GetRequiredService<IDataSourceFactory>();
        var connectionStringBuilder = scope.ServiceProvider.GetRequiredService<IConnectionStringBuilder>();

        // BuildConnectionStringAsync returns a SecretValue whose internal char[] buffer is
        // zeroed on Dispose(), keeping the resolved password off the heap as a plain string.
        using var connSecret = await connectionStringBuilder.BuildConnectionStringAsync(connectionStringTemplate!, ct);

        if (connSecret.IsEmpty)
            throw new TransformationException(
                $"Connection string '{def.ConnectionStringName}' not found or empty for reference key '{key}'.");

        _logger.LogInformation(
            "ReferenceDataProvider: Loading '{Key}' from {SourceType} ({ConnName}) with TTL {TtlMinutes} min",
            key, def.SourceType, def.ConnectionStringName, def.Ttl.TotalMinutes);
        var ds = dataSourceFactory.Create(def.SourceType);

        var dt = await ds.ExtractAsync(
            connectionStringFactory: () => connSecret.Expose(),   // Expose() is called inline inside the connection ctor; connSecret is still alive in the using scope
            query: def.QueryOrCommand,
            extractionType: def.ExtractionType,
            parameters: def.Parameters,
            cancellationToken: ct);

        var expires = DateTime.UtcNow.Add(def.Ttl);

        _logger.LogInformation(
            "ReferenceDataProvider: Loaded '{Key}' ({Rows} rows, {Cols} cols). Cache expires at {Expires:o}",
            key, dt.Rows.Count, dt.Columns.Count, expires);

        return new CacheEntry(dt, expires);
    }

    public async Task<DateTime?> GetDateAsync(string key, CancellationToken ct)
    {
        var dt = await GetAsync(key, ct);

        if (dt.Rows.Count == 0 || dt.Columns.Count == 0)
            return null;

        var raw = dt.Rows[0][0];

        return raw is DBNull || raw is null ? null : Convert.ToDateTime(raw);
    }

}
