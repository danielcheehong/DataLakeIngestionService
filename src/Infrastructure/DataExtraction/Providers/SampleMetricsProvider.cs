using System.Data;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.DataExtraction.Providers;

/// <summary>
/// Sample DotNet data provider that generates metrics data.
/// This serves as an example implementation of IDotNetDataProvider.
/// </summary>
public class SampleMetricsProvider : IDotNetDataProvider
{
    private readonly ILogger<SampleMetricsProvider> _logger;

    public SampleMetricsProvider(ILogger<SampleMetricsProvider> logger)
    {
        _logger = logger;
    }

    public string ProviderName => "SampleMetrics";

    public Task<DataTable> GenerateDataAsync(
        Dictionary<string, object>? parameters,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Generating sample metrics data");

        var dataTable = new DataTable("Metrics");

        // Define schema
        dataTable.Columns.Add("MetricId", typeof(int));
        dataTable.Columns.Add("MetricName", typeof(string));
        dataTable.Columns.Add("Value", typeof(decimal));
        dataTable.Columns.Add("Timestamp", typeof(DateTime));
        dataTable.Columns.Add("Category", typeof(string));

        // Get parameters with defaults
        var startDate = DateTime.Today.AddDays(-7);
        var endDate = DateTime.Today;

        if (parameters?.TryGetValue("startDate", out var sd) == true && sd != null)
        {
            if (DateTime.TryParse(sd.ToString(), out var parsedStart))
                startDate = parsedStart;
        }

        if (parameters?.TryGetValue("endDate", out var ed) == true && ed != null)
        {
            if (DateTime.TryParse(ed.ToString(), out var parsedEnd))
                endDate = parsedEnd;
        }

        _logger.LogDebug("Generating metrics for date range: {StartDate} to {EndDate}", 
            startDate.ToString("yyyy-MM-dd"), 
            endDate.ToString("yyyy-MM-dd"));

        // Generate sample data
        var random = new Random(42); // Fixed seed for reproducible results
        var categories = new[] { "Performance", "Usage", "Error", "Latency" };
        var metricId = 1;

        for (var date = startDate; date <= endDate; date = date.AddDays(1))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (var category in categories)
            {
                dataTable.Rows.Add(
                    metricId++,
                    $"{category}Metric",
                    Math.Round((decimal)(random.NextDouble() * 100), 2),
                    date,
                    category);
            }
        }

        _logger.LogInformation("Generated {RowCount} metric rows for {DayCount} days",
            dataTable.Rows.Count,
            (endDate - startDate).Days + 1);

        return Task.FromResult(dataTable);
    }
}
