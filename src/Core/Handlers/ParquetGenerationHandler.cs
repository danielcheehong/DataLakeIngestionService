using System.Diagnostics;
using DataLakeIngestionService.Core.Enums;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Parquet;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Core.Handlers;

public class ParquetGenerationHandler : BasePipelineHandler
{
    private readonly IParquetWriter _parquetWriter;

    public ParquetGenerationHandler(
        IParquetWriter parquetWriter,
        ILogger<ParquetGenerationHandler> logger) : base(logger)
    {
        _parquetWriter = parquetWriter;
    }

    public override string StageName => "ParquetGeneration";

    protected override async Task<PipelineResult> ExecuteAsync(IPipelineContext context)
    {
        if (context.ExtractedData == null)
        {
            return new PipelineResult
            {
                IsSuccess = false,
                Message = "No data available for Parquet generation",
                ShouldContinue = false
            };
        }

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Generate Parquet file
            using var memoryStream = new MemoryStream();
            await _parquetWriter.WriteAsync(context.ExtractedData, memoryStream, context.CancellationToken);

            context.ParquetData = memoryStream.ToArray();

            stopwatch.Stop();

            Logger.LogInformation(
                "Generated Parquet file: {FileSize} bytes in {ElapsedMs}ms",
                context.ParquetData.Length,
                stopwatch.ElapsedMilliseconds);

            return new PipelineResult
            {
                IsSuccess = true,
                Message = $"Generated Parquet file ({context.ParquetData.Length} bytes)",
                ShouldContinue = true,
                StageMetrics = new Dictionary<string, object>
                {
                    ["FileSizeBytes"] = context.ParquetData.Length,
                    ["DurationMs"] = stopwatch.ElapsedMilliseconds
                }
            };
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Parquet generation failed");

            context.Errors.Add(new PipelineError
            {
                Stage = StageName,
                Message = "Parquet file generation failed",
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
