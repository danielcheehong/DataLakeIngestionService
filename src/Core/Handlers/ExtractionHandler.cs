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
            // Check if multiple sources are configured
            if (context.Metadata.TryGetValue("SourceConfigurations", out var sourcesObj) 
                && sourcesObj is List<SourceExtractionConfig> sourceConfigs 
                && sourceConfigs.Count > 0)
            {
                return await ExtractMultipleSourcesAsync(context, sourceConfigs, stopwatch);
            }
            
            // Single source extraction (backward compatibility)
            return await ExtractSingleSourceAsync(context, stopwatch);
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

    private async Task<PipelineResult> ExtractSingleSourceAsync(
        IPipelineContext context, 
        Stopwatch stopwatch)
    {
        var sourceType = context.Metadata["SourceType"]?.ToString() 
            ?? throw new ExtractionException("SourceType not found in metadata");
        var extractionType = context.Metadata.TryGetValue("ExtractionType", out var et) && et != null
            ? (ExtractionType)et
            : throw new ExtractionException("ExtractionType not found in metadata");
        var connectionString = context.Metadata["ConnectionString"]?.ToString() 
            ?? throw new ExtractionException("ConnectionString not found in metadata");
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ExtractionException("ConnectionString is empty or whitespace in metadata");
        var query = context.Metadata["Query"]?.ToString() 
            ?? throw new ExtractionException("Query not found in metadata");
        var parameters = context.Metadata.TryGetValue("Parameters", out var p) 
            ? p as Dictionary<string, object> 
            : new Dictionary<string, object>();

        var dataSource = _dataSourceFactory.Create(sourceType);
        
        var extractedData = await dataSource.ExtractAsync(
            connectionString,
            query,
            extractionType,
            parameters,
            context.CancellationToken);

        // Store in both ExtractedData (for backward compatibility) and ExtractedDataSets
        context.ExtractedData = extractedData;
        
        var sourceId = context.Metadata.TryGetValue("SourceId", out var sid) 
            ? sid?.ToString() ?? "default" 
            : "default";
        context.ExtractedDataSets[sourceId] = extractedData;

        stopwatch.Stop();

        Logger.LogInformation(
            "Extracted {RowCount} rows from single source in {ElapsedMs}ms",
            extractedData.Rows.Count,
            stopwatch.ElapsedMilliseconds);

        return new PipelineResult
        {
            IsSuccess = true,
            Message = $"Extracted {extractedData.Rows.Count} rows",
            ShouldContinue = true,
            StageMetrics = new Dictionary<string, object>
            {
                ["RowCount"] = extractedData.Rows.Count,
                ["DurationMs"] = stopwatch.ElapsedMilliseconds
            }
        };
    }

    private async Task<PipelineResult> ExtractMultipleSourcesAsync(
        IPipelineContext context,
        List<SourceExtractionConfig> sourceConfigs,
        Stopwatch stopwatch)
    {
        Logger.LogInformation("Extracting from {Count} sources", sourceConfigs.Count);
        
        var totalRows = 0;
        var sourceMetrics = new Dictionary<string, object>();

        foreach (var sourceConfig in sourceConfigs)
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            
            Logger.LogInformation(
                "Extracting from source '{SourceId}' ({SourceType})",
                sourceConfig.SourceId,
                sourceConfig.SourceType);

            var dataSource = _dataSourceFactory.Create(sourceConfig.SourceType);
            
            var extractedData = await dataSource.ExtractAsync(
                sourceConfig.ConnectionString,
                sourceConfig.Query,
                sourceConfig.ExtractionType,
                sourceConfig.Parameters,
                context.CancellationToken);

            context.ExtractedDataSets[sourceConfig.SourceId] = extractedData;
            totalRows += extractedData.Rows.Count;

            Logger.LogInformation(
                "Extracted {RowCount} rows from source '{SourceId}'",
                extractedData.Rows.Count,
                sourceConfig.SourceId);

            sourceMetrics[$"{sourceConfig.SourceId}_RowCount"] = extractedData.Rows.Count;
        }

        stopwatch.Stop();

        Logger.LogInformation(
            "Extracted {TotalRows} total rows from {SourceCount} sources in {ElapsedMs}ms",
            totalRows,
            sourceConfigs.Count,
            stopwatch.ElapsedMilliseconds);

        sourceMetrics["TotalRowCount"] = totalRows;
        sourceMetrics["SourceCount"] = sourceConfigs.Count;
        sourceMetrics["DurationMs"] = stopwatch.ElapsedMilliseconds;

        return new PipelineResult
        {
            IsSuccess = true,
            Message = $"Extracted {totalRows} total rows from {sourceConfigs.Count} sources",
            ShouldContinue = true,
            StageMetrics = sourceMetrics
        };
    }
}
