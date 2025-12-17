using System.Diagnostics;
using DataLakeIngestionService.Core.Enums;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using DataLakeIngestionService.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Core.Handlers;

public class ExtractionHandler : BasePipelineHandler
{
    private readonly IDataSourceFactory _dataSourceFactory;

    public ExtractionHandler(
        IDataSourceFactory dataSourceFactory,
        ILogger<ExtractionHandler> logger) : base(logger)
    {
        _dataSourceFactory = dataSourceFactory;
    }

    public override string StageName => "Extraction";

    protected override async Task<PipelineResult> ExecuteAsync(IPipelineContext context)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Get configuration from context metadata
            var sourceType = context.Metadata["SourceType"]?.ToString() ?? throw new ExtractionException("SourceType not found in metadata");
            var connectionString = context.Metadata["ConnectionString"]?.ToString() ?? throw new ExtractionException("ConnectionString not found in metadata");
            var query = context.Metadata["Query"]?.ToString() ?? throw new ExtractionException("Query not found in metadata");
            var parameters = context.Metadata.TryGetValue("Parameters", out var p) 
                ? p as Dictionary<string, object> 
                : new Dictionary<string, object>();

            // Execute extraction
            var dataSource = _dataSourceFactory.Create(sourceType);
            context.ExtractedData = await dataSource.ExtractAsync(
                connectionString,
                query,
                parameters,
                context.CancellationToken);

            stopwatch.Stop();

            Logger.LogInformation(
                "Extracted {RowCount} rows in {ElapsedMs}ms",
                context.ExtractedData.Rows.Count,
                stopwatch.ElapsedMilliseconds);

            return new PipelineResult
            {
                IsSuccess = true,
                Message = $"Extracted {context.ExtractedData.Rows.Count} rows",
                ShouldContinue = true,
                StageMetrics = new Dictionary<string, object>
                {
                    ["RowCount"] = context.ExtractedData.Rows.Count,
                    ["DurationMs"] = stopwatch.ElapsedMilliseconds
                }
            };
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Extraction failed");

            context.Errors.Add(new PipelineError
            {
                Stage = StageName,
                Message = "Data extraction failed",
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
