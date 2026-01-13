using System.Diagnostics;
using System.Security.Cryptography;
using DataLakeIngestionService.Core.Enums;
using DataLakeIngestionService.Core.Interfaces.Parquet;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Core.Handlers;

/// <summary>
/// Pipeline handler that generates CTL (control) files for Parquet data.
/// The CTL file contains metadata: RecordCount, RefDate, Checksum (SHA256), Timestamp, DatasetName, Source.
/// </summary>
public class CtlGenerationHandler : BasePipelineHandler
{
    private readonly ICtlWriter _ctlWriter;

    public CtlGenerationHandler(
        ICtlWriter ctlWriter,
        ILogger<CtlGenerationHandler> logger) : base(logger)
    {
        _ctlWriter = ctlWriter;
    }

    public override string StageName => "CtlGeneration";

    protected override async Task<PipelineResult> ExecuteAsync(IPipelineContext context)
    {
        if (context.ParquetData == null || context.ParquetData.Length == 0)
        {
            return new PipelineResult
            {
                IsSuccess = false,
                Message = "No Parquet data available for CTL generation",
                ShouldContinue = false
            };
        }

        var stopwatch = Stopwatch.StartNew();

        try
        {
            var now = DateTime.UtcNow;

            // Get dataset ID from metadata
            var datasetId = context.Metadata.TryGetValue("DatasetId", out var id)
                ? id?.ToString() ?? "unknown"
                : "unknown";

            // Get source from metadata (e.g., "SqlServer", "Oracle")
            var source = context.Metadata.TryGetValue("SourceType", out var src)
                ? src?.ToString() ?? string.Empty
                : string.Empty;

            // Compute SHA256 checksum of Parquet data
            var checksumBytes = SHA256.HashData(context.ParquetData);
            var checksum = Convert.ToHexString(checksumBytes).ToLowerInvariant();

            // Get row count from extracted data
            var recordCount = context.ExtractedData?.Rows.Count ?? 0;

            // Build dataset name: {datasetId}_{yyyyMMddHHmmss}
            var timestamp = now.ToString("yyyyMMddHHmmss");
            var datasetName = $"{datasetId}_{timestamp}";

            // Create CTL record with all required fields
            var ctlRecord = new CtlRecord
            {
                RecordCount = recordCount,
                RefDate = now.ToString("o"),        // ISO 8601 format
                Checksum = checksum,
                Timestamp = now.ToString("o"),      // ISO 8601 format
                DatasetName = datasetName,
                Source = source
            };

            // Generate CTL file content
            context.CtlData = await _ctlWriter.WriteAsync(ctlRecord, context.CancellationToken);
            context.CtlFileName = $"{datasetName}.ctl";

            stopwatch.Stop();

            Logger.LogInformation(
                "Generated CTL file: {FileName}, RecordCount: {RecordCount}, Checksum: {Checksum}, Duration: {ElapsedMs}ms",
                context.CtlFileName,
                recordCount,
                checksum,
                stopwatch.ElapsedMilliseconds);

            return new PipelineResult
            {
                IsSuccess = true,
                Message = $"Generated CTL file ({context.CtlData.Length} bytes)",
                ShouldContinue = true,
                StageMetrics = new Dictionary<string, object>
                {
                    ["CtlFileName"] = context.CtlFileName,
                    ["RecordCount"] = recordCount,
                    ["Checksum"] = checksum,
                    ["FileSizeBytes"] = context.CtlData.Length,
                    ["DurationMs"] = stopwatch.ElapsedMilliseconds
                }
            };
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "CTL generation failed");

            context.Errors.Add(new PipelineError
            {
                Stage = StageName,
                Message = "CTL file generation failed",
                Exception = ex,
                Timestamp = DateTime.UtcNow,
                Severity = ErrorSeverity.Critical
            });

            return new PipelineResult
            {
                IsSuccess = false,
                Message = ex.Message,
                ShouldContinue = false
            };
        }
    }
}
