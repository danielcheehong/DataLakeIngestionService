using System.Text;
using DataLakeIngestionService.Core.Interfaces.Parquet;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Parquet;

/// <summary>
/// Service for generating CTL (control) files in CSV format.
/// </summary>
public class CtlWriterService : ICtlWriter
{
    private readonly ILogger<CtlWriterService> _logger;

    public CtlWriterService(ILogger<CtlWriterService> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public Task<byte[]> WriteAsync(CtlRecord record, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var sb = new StringBuilder();

        // Write CSV header
        sb.AppendLine("RecordCount,RefDate,Checksum,Timestamp,DatasetName,Source");

        // Write data row
        sb.AppendLine(string.Join(",",
            record.RecordCount.ToString(),
            EscapeCsvField(record.RefDate),
            EscapeCsvField(record.Checksum),
            EscapeCsvField(record.Timestamp),
            EscapeCsvField(record.DatasetName),
            EscapeCsvField(record.Source)));

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());

        _logger.LogDebug(
            "Generated CTL content: RecordCount={RecordCount}, DatasetName={DatasetName}, Checksum={Checksum}",
            record.RecordCount,
            record.DatasetName,
            record.Checksum);

        return Task.FromResult(bytes);
    }

    /// <summary>
    /// Escapes a CSV field value according to RFC 4180.
    /// </summary>
    private static string EscapeCsvField(string value)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;

        // If field contains comma, quote, or newline, wrap in quotes and escape existing quotes
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
        {
            return $"\"{value.Replace("\"", "\"\"")}\"";
        }

        return value;
    }
}
