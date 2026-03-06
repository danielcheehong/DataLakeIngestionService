using System.Diagnostics;

namespace DataLakeIngestionService.Core.Pipeline;

public static class PipelineActivitySource
{
    public const string Name = "DataLakeIngestionService";
    public static readonly ActivitySource Source = new(Name, "1.0.0");
}
