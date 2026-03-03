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
    private readonly IConnectionStringBuilder _connectionStringBuilder;
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
        IConnectionStringBuilder connectionStringBuilder,
        IConfiguration configuration,
        IHostEnvironment env)
    {
        _logger = logger;
        _connectionStringBuilder = connectionStringBuilder;
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
                Ttl: TimeSpan.FromMinutes(10))
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
                new Lazy<Task<CacheEntry>>(() => LoadAndCacheAsync(key, def, ct)));

            CacheEntry entry;
            try
            {
                entry = await lazy.Value;
            }
            catch
            {
                // failed load should not poison cache
                _cache.TryRemove(cacheKey, out _);
                throw;
            }

            if (DateTime.UtcNow <= entry.ExpiresUtc)
            {
                // Return a copy so callers can't mutate the cached master
                return entry.Master.Copy();
            }

            // expired: remove and loop to reload
            _cache.TryRemove(cacheKey, out _);
        }
    }

    private async Task<CacheEntry> LoadAndCacheAsync(string key, ReferenceDefinition def, CancellationToken ct)
    {
        var connectionStringTemplate = _configuration.GetConnectionString(def.ConnectionStringName);

        // BuildConnectionStringAsync returns a SecretValue whose internal char[] buffer is
        // zeroed on Dispose(), keeping the resolved password off the heap as a plain string.
        using var connSecret = await _connectionStringBuilder.BuildConnectionStringAsync(connectionStringTemplate!, ct);

        if (connSecret.IsEmpty)
            throw new TransformationException(
                $"Connection string '{def.ConnectionStringName}' not found or empty for reference key '{key}'.");

        _logger.LogInformation(
            "ReferenceDataProvider: Loading '{Key}' from {SourceType} ({ConnName}) with TTL {TtlMinutes} min",
            key, def.SourceType, def.ConnectionStringName, def.Ttl.TotalMinutes);

        using var scope = _scopeFactory.CreateScope();
        var dataSourceFactory = scope.ServiceProvider.GetRequiredService<IDataSourceFactory>();
        var ds = dataSourceFactory.Create(def.SourceType);

        var dt = await ds.ExtractAsync(
            connectionString: connSecret.Expose(),   // materialises the string only for this call; connSecret is disposed at the end of this method
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

}
