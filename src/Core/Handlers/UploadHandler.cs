using System.Diagnostics;
using DataLakeIngestionService.Core.Enums;
using DataLakeIngestionService.Core.Exceptions;
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
            var fileName = context.Metadata["FileName"]?.ToString() ?? $"{context.JobId}.parquet";

            var provider = _uploadProviderFactory.Create(uploadProvider);
            var uploadResult = await provider.UploadAsync(
                context.ParquetData,
                destinationPath,
                fileName,
                context.CancellationToken);

            context.UploadUri = uploadResult.Path;

            stopwatch.Stop();

            Logger.LogInformation(
                "Uploaded {FileSize} bytes to {Uri} in {ElapsedMs}ms",
                context.ParquetData.Length,
                uploadResult.Path,
                stopwatch.ElapsedMilliseconds);

            return new PipelineResult
            {
                IsSuccess = true,
                Message = $"Uploaded to {uploadResult.Path}",
                ShouldContinue = true,
                StageMetrics = new Dictionary<string, object>
                {
                    ["UploadUri"] = uploadResult.Path,
                    ["FileSizeBytes"] = context.ParquetData.Length,
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
}
