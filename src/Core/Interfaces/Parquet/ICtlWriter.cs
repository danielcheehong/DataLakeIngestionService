namespace DataLakeIngestionService.Core.Interfaces.Parquet;

/// <summary>
/// Interface for writing CTL (control) files that accompany Parquet files.
/// </summary>
public interface ICtlWriter
{
    /// <summary>
    /// Writes a CTL record to a byte array in CSV format.
    /// </summary>
    /// <param name="record">The CTL record containing metadata about the Parquet file.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Byte array containing the CSV-formatted CTL file content.</returns>
    Task<byte[]> WriteAsync(CtlRecord record, CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents the metadata record for a CTL control file.
/// </summary>
public class CtlRecord
{
    /// <summary>
    /// Number of rows in the Parquet file.
    /// </summary>
    public int RecordCount { get; set; }

    /// <summary>
    /// Reference date in ISO 8601 format (e.g., "2026-01-12T10:30:00.0000000Z").
    /// </summary>
    public string RefDate { get; set; } = string.Empty;

    /// <summary>
    /// SHA256 checksum of the Parquet file in hexadecimal format.
    /// </summary>
    public string Checksum { get; set; } = string.Empty;

    /// <summary>
    /// Timestamp when CTL file was generated in ISO 8601 format.
    /// </summary>
    public string Timestamp { get; set; } = string.Empty;

    /// <summary>
    /// Dataset identifier with timestamp: {datasetId}_{yyyyMMddHHmmss}.
    /// </summary>
    public string DatasetName { get; set; } = string.Empty;

    /// <summary>
    /// Source system identifier.
    /// </summary>
    public string Source { get; set; } = string.Empty;
}
