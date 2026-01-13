using System.Diagnostics;
using DataLakeIngestionService.Core.Enums;
using DataLakeIngestionService.Core.Interfaces.Upload;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Core.Handlers;

public class UploadHandler : BasePipelineHandler
{
    private readonly IUploadProviderFactory _uploadProviderFactory;

    public UploadHandler(
        IUploadProviderFactory uploadProviderFactory,
        ILogger<UploadHandler> logger) : base(logger)
    {
        _uploadProviderFactory = uploadProviderFactory;
    }

    public override string StageName => "Upload";

    protected override async Task<PipelineResult> ExecuteAsync(IPipelineContext context)
    {
        if (context.ParquetData == null || context.ParquetData.Length == 0)
        {
            return new PipelineResult
            {
                IsSuccess = false,
                Message = "No Parquet data available for upload",
                ShouldContinue = false
            };
        }

        var stopwatch = Stopwatch.StartNew();

        try
        {
            var uploadProvider = context.Metadata["UploadProvider"]?.ToString() ?? "FileSystem";
            var destinationPath = context.Metadata["DestinationPath"]?.ToString() ?? string.Empty;
            var parquetFileName = context.Metadata["FileName"]?.ToString() ?? $"{context.JobId}.parquet";
            var ctlFileName = context.CtlFileName ?? $"{context.JobId}.ctl";

            // Get local copy settings from metadata
            var keepLocalCopy = context.Metadata.TryGetValue("KeepLocalCopy", out var klc) && klc is true;
            var localCopyPath = context.Metadata.TryGetValue("LocalCopyPath", out var lcp)
                ? lcp?.ToString() ?? string.Empty
                : string.Empty;

            var provider = _uploadProviderFactory.Create(uploadProvider);

            // Upload Parquet file
            var parquetUploadResult = await provider.UploadAsync(
                context.ParquetData,
                destinationPath,
                parquetFileName,
                context.CancellationToken);

            context.UploadUri = parquetUploadResult.Path;

            Logger.LogInformation(
                "Uploaded Parquet file: {FileSize} bytes to {Uri}",
                context.ParquetData.Length,
                parquetUploadResult.Path);

            // Upload CTL file if available
            string? ctlUploadUri = null;
            if (context.CtlData != null && context.CtlData.Length > 0)
            {
                var ctlUploadResult = await provider.UploadAsync(
                    context.CtlData,
                    destinationPath,
                    ctlFileName,
                    context.CancellationToken);

                ctlUploadUri = ctlUploadResult.Path;

                Logger.LogInformation(
                    "Uploaded CTL file: {FileSize} bytes to {Uri}",
                    context.CtlData.Length,
                    ctlUploadResult.Path);
            }

            // Keep local copy if configured
            if (keepLocalCopy && !string.IsNullOrWhiteSpace(localCopyPath))
            {
                await SaveLocalCopyAsync(context, localCopyPath, parquetFileName, ctlFileName);
            }

            stopwatch.Stop();

            return new PipelineResult
            {
                IsSuccess = true,
                Message = $"Uploaded to {parquetUploadResult.Path}",
                ShouldContinue = true,
                StageMetrics = new Dictionary<string, object>
                {
                    ["ParquetUploadUri"] = parquetUploadResult.Path,
                    ["CtlUploadUri"] = ctlUploadUri ?? "N/A",
                    ["ParquetFileSizeBytes"] = context.ParquetData.Length,
                    ["CtlFileSizeBytes"] = context.CtlData?.Length ?? 0,
                    ["DurationMs"] = stopwatch.ElapsedMilliseconds
                }
            };
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Upload failed");

            context.Errors.Add(new PipelineError
            {
                Stage = StageName,
                Message = "File upload failed",
                Exception = ex,
                Timestamp = DateTime.UtcNow,
                Severity = ErrorSeverity.Error
            });

            return new PipelineResult
            {
                IsSuccess = false,
                Message = ex.Message,
                ShouldContinue = false
            };
        }
    }

    /// <summary>
    /// Saves local copies of Parquet and CTL files. Logs errors but does not fail the pipeline.
    /// </summary>
    private async Task SaveLocalCopyAsync(
        IPipelineContext context,
        string localCopyPath,
        string parquetFileName,
        string ctlFileName)
    {
        try
        {
            // Ensure directory exists
            Directory.CreateDirectory(localCopyPath);

            // Save Parquet file locally
            if (context.ParquetData != null)
            {
                var parquetPath = Path.Combine(localCopyPath, parquetFileName);
                await File.WriteAllBytesAsync(parquetPath, context.ParquetData, context.CancellationToken);
                Logger.LogInformation("Saved local copy of Parquet file: {Path}", parquetPath);
            }

            // Save CTL file locally
            if (context.CtlData != null)
            {
                var ctlPath = Path.Combine(localCopyPath, ctlFileName);
                await File.WriteAllBytesAsync(ctlPath, context.CtlData, context.CancellationToken);
                Logger.LogInformation("Saved local copy of CTL file: {Path}", ctlPath);
            }
        }
        catch (Exception ex)
        {
            // Log error but don't fail the pipeline - local copy is non-critical
            Logger.LogError(ex, "Failed to save local copy to {LocalCopyPath}. Pipeline will continue.", localCopyPath);
        }
    }
}
