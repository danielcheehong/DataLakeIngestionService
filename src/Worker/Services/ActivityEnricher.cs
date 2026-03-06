using System.Diagnostics;
using Serilog.Core;
using Serilog.Events;

namespace DataLakeIngestionService.Worker.Services;

/// <summary>
/// Enriches Serilog log events with TraceId and SpanId from the current
/// <see cref="Activity"/>, making every log line correlatable to OTel traces.
/// </summary>
internal sealed class ActivityEnricher : ILogEventEnricher
{
    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
        var activity = Activity.Current;
        if (activity is null) return;

        logEvent.AddPropertyIfAbsent(
            propertyFactory.CreateProperty("TraceId", activity.TraceId.ToString()));
        logEvent.AddPropertyIfAbsent(
            propertyFactory.CreateProperty("SpanId", activity.SpanId.ToString()));
    }
}
