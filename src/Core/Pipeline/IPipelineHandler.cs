namespace DataLakeIngestionService.Core.Pipeline;

public interface IPipelineHandler
{
    string StageName { get; }
    Task<PipelineResult> HandleAsync(IPipelineContext context);
    IPipelineHandler? SetNext(IPipelineHandler handler);
}
