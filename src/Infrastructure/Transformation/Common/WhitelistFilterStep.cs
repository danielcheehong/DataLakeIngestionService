using System.Collections.Concurrent;
using System.Data;
using System.Globalization;
using System.Text;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Transformation.Common;

/// <summary>
/// Transformation step that filters rows in the DataTable based on a whitelist of allowed account identifiers loaded from a CSV file.
/// Python parity: if the target column does NOT exist in the table, the step will NOT filter (pass-through).
/// </summary>
public class WhitelistFilterStep : ITransformationStep
{
    private readonly ILogger<WhitelistFilterStep> _logger;
    private readonly Dictionary<string, object> _config;

    // Simple static cache: key -> (lastWriteUtc, whitelistSet)
    // Key includes csvPath + delimiter + encoding + columnName to avoid collisions.
    private static readonly ConcurrentDictionary<string, CacheEntry> _whitelistCache = new();

    private sealed record CacheEntry(DateTime LastWriteUtc, HashSet<string> Allowed);

    public WhitelistFilterStep(
        ILogger<WhitelistFilterStep> logger,
        Dictionary<string, object>? config = null)
    {
        _logger = logger;
        _config = config ?? new Dictionary<string, object>();
    }

    public string Name => "WhitelistFilter";
    public List<string> Environments { get; set; } = new();

    public async Task TransformAsync(IPipelineContext context, CancellationToken cancellationToken)
    {
        var csvPath = GetConfigValue<string>("csvPath", string.Empty);
        var accountIdColumn = GetConfigValue<string>("accountIdColumn", "ACCOUNT_ID");
        var delimiterStr = GetConfigValue<string>("delimiter", ",");     // config might provide "," or ";"
        var encodingName = GetConfigValue<string>("encoding", "utf-8");

        if (string.IsNullOrWhiteSpace(csvPath))
            throw new TransformationException("csvPath must be provided in WhitelistFilterStep config.");

        if (!File.Exists(csvPath))
            throw new TransformationException($"Whitelist CSV file not found: {csvPath}");

        if (context.ExtractedData == null)
            throw new TransformationException("ExtractedData is null before whitelist filtering.");

        var table = context.ExtractedData;

        // --- Python parity: if the table does not have the column, do nothing (pass-through)
        if (!table.Columns.Contains(accountIdColumn))
        {
            _logger.LogInformation(
                "WhitelistFilterStep: Column '{Column}' not found in ExtractedData. Skipping filtering (python parity).",
                accountIdColumn);

            return;
        }

        // Delimiter: treat config as a single char delimiter for split
        if (string.IsNullOrEmpty(delimiterStr))
            delimiterStr = ",";
        char delimiter = delimiterStr[0];

        Encoding encoding;
        try
        {
            encoding = Encoding.GetEncoding(encodingName);
        }
        catch (Exception ex)
        {
            throw new TransformationException($"Invalid encoding '{encodingName}'.", ex);
        }

        // Load whitelist (from cache if unchanged)
        var allowedAccounts = await GetOrLoadWhitelistAsync(
            csvPath,
            accountIdColumn,
            delimiter,
            encoding,
            _logger,
            cancellationToken);

        int beforeCount = table.Rows.Count;

        // Filter by building a new DataTable (faster & safer than removing rows one-by-one)
        var filtered = table.Clone();

        int nullOrEmptyCount = 0;
        int notAllowedCount = 0;

        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var id = NormalizeId(row[accountIdColumn]);

            if (string.IsNullOrEmpty(id))
            {
                nullOrEmptyCount++;
                continue;
            }

            if (!allowedAccounts.Contains(id))
            {
                notAllowedCount++;
                continue;
            }

            filtered.ImportRow(row);
        }

        context.ExtractedData = filtered;

        _logger.LogInformation(
            "WhitelistFilterStep: Filtered rows from {Before} to {After}. Dropped empty IDs: {Empty}. Dropped not allowed: {NotAllowed}. Whitelist size: {WhitelistCount}.",
            beforeCount, filtered.Rows.Count, nullOrEmptyCount, notAllowedCount, allowedAccounts.Count);
    }

    /// <summary>
    /// Gets cached whitelist if the CSV file hasn't changed; otherwise reloads it.
    /// </summary>
    private static async Task<HashSet<string>> GetOrLoadWhitelistAsync(
        string csvPath,
        string accountIdColumn,
        char delimiter,
        Encoding encoding,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        // Cache key includes parsing/config signature
        var cacheKey = $"{csvPath}||{accountIdColumn}||{delimiter}||{encoding.WebName}".ToLowerInvariant();

        DateTime lastWriteUtc;
        try
        {
            lastWriteUtc = File.GetLastWriteTimeUtc(csvPath);
        }
        catch (Exception ex)
        {
            throw new TransformationException($"Unable to read last write time for whitelist CSV: {csvPath}", ex);
        }

        if (_whitelistCache.TryGetValue(cacheKey, out var cached) && cached.LastWriteUtc == lastWriteUtc)
        {
            logger.LogDebug("WhitelistFilterStep: Using cached whitelist for {CsvPath}", csvPath);
            return cached.Allowed;
        }

        var loaded = await LoadWhitelistFromCsvAsync(csvPath, accountIdColumn, delimiter, encoding, cancellationToken);

        // Replace cache entry
        _whitelistCache[cacheKey] = new CacheEntry(lastWriteUtc, loaded);

        logger.LogInformation("WhitelistFilterStep: Loaded {Count} allowed IDs from {CsvPath}", loaded.Count, csvPath);
        return loaded;
    }

    /// <summary>
    /// Loads allowed account IDs from a CSV file into a HashSet.
    /// NOTE: Uses simple Split parsing; if CSV may contain quoted delimiters, use a CSV library (e.g., CsvHelper).
    /// </summary>
    private static async Task<HashSet<string>> LoadWhitelistFromCsvAsync(
        string csvPath,
        string accountIdColumn,
        char delimiter,
        Encoding encoding,
        CancellationToken cancellationToken)
    {
        var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        using var reader = new StreamReader(csvPath, encoding);

        var headerLine = await reader.ReadLineAsync(cancellationToken);
        if (headerLine == null)
            throw new TransformationException($"CSV file {csvPath} is empty.");

        var headers = headerLine.Split(delimiter);
        var idIndex = Array.FindIndex(headers, h => string.Equals(h.Trim(), accountIdColumn, StringComparison.OrdinalIgnoreCase));
        if (idIndex == -1)
            throw new TransformationException($"Account ID column '{accountIdColumn}' not found in CSV header.");

        string? line;
        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(line))
                continue;

            var parts = line.Split(delimiter);
            if (parts.Length <= idIndex)
                continue;

            var id = NormalizeId(parts[idIndex]);
            if (!string.IsNullOrEmpty(id))
                allowed.Add(id);
        }

        return allowed;
    }

    /// <summary>
    /// Normalizes IDs for matching across CSV + DataTable:
    /// - handles DBNull
    /// - trims
    /// - converts NBSP to normal space before trimming
    /// </summary>
    private static string NormalizeId(object? value)
    {
        if (value == null || value == DBNull.Value)
            return string.Empty;

        return (value.ToString() ?? string.Empty)
            .Replace('\u00A0', ' ')
            .Trim();
    }

    private T GetConfigValue<T>(string key, T defaultValue)
    {
        if (!_config.TryGetValue(key, out var value))
            return defaultValue;

        if (value is T tValue)
            return tValue;

        try
        {
            return (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
        }
        catch
        {
            return defaultValue;
        }
    }
}
