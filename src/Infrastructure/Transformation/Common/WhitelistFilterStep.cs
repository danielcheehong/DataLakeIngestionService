using System.Data;
using System.Globalization;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Transformation.Common;

/// <summary>
/// Transformation step that filters rows in the DataTable based on a whitelist of allowed account identifiers loaded from a CSV file.
/// </summary>
public class WhitelistFilterStep : ITransformationStep
{
    private readonly ILogger<WhitelistFilterStep> _logger;
    private readonly Dictionary<string, object> _config;

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
        var delimiter = GetConfigValue<string>("delimiter", ",");
        var encodingName = GetConfigValue<string>("encoding", "utf-8");

        if (string.IsNullOrWhiteSpace(csvPath))
            throw new TransformationException("csvPath must be provided in WhitelistFilterStep config.");

        if (!File.Exists(csvPath))
            throw new TransformationException($"Whitelist CSV file not found: {csvPath}");

        var allowedAccounts = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var encoding = System.Text.Encoding.GetEncoding(encodingName);

        // Load allowed account IDs from CSV
        using (var reader = new StreamReader(csvPath, encoding))
        {
            string? headerLine = await reader.ReadLineAsync();
            if (headerLine == null)
                throw new TransformationException($"CSV file {csvPath} is empty.");

            var headers = headerLine.Split(delimiter);
            int idIndex = Array.FindIndex(headers, h => string.Equals(h, accountIdColumn, StringComparison.OrdinalIgnoreCase));
            if (idIndex == -1)
                throw new TransformationException($"Account ID column '{accountIdColumn}' not found in CSV header.");

            string? line;
            while ((line = await reader.ReadLineAsync()) != null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                var parts = line.Split(delimiter);
                if (parts.Length > idIndex)
                {
                    var id = parts[idIndex].Trim();
                    if (!string.IsNullOrEmpty(id))
                        allowedAccounts.Add(id);
                }
            }
        }

        _logger.LogInformation("Loaded {Count} allowed account IDs from {CsvPath}", allowedAccounts.Count, csvPath);

        // Filter ExtractedData
        if (context.ExtractedData == null)
            throw new TransformationException("ExtractedData is null before whitelist filtering.");

        var table = context.ExtractedData;
        if (!table.Columns.Contains(accountIdColumn))
            throw new TransformationException($"ExtractedData does not contain column '{accountIdColumn}' for filtering.");

        int beforeCount = table.Rows.Count;
        var rowsToRemove = new List<DataRow>();
        foreach (DataRow row in table.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var id = row[accountIdColumn]?.ToString();
            if (string.IsNullOrEmpty(id) || !allowedAccounts.Contains(id))
                rowsToRemove.Add(row);
        }
        foreach (var row in rowsToRemove)
            table.Rows.Remove(row);

        int afterCount = table.Rows.Count;
        _logger.LogInformation("WhitelistFilterStep: Filtered rows from {Before} to {After}", beforeCount, afterCount);
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
