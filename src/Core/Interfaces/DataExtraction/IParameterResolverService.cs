namespace DataLakeIngestionService.Core.Interfaces.DataExtraction;

/// <summary>
/// Resolves parameter placeholders to runtime values.
/// Supports expressions like ${today}, ${today-1}, ${env:VAR_NAME}.
/// </summary>
public interface IParameterResolverService
{
    /// <summary>
    /// Resolves placeholder expressions in parameters to actual values.
    /// </summary>
    /// <param name="parameters">Raw parameters from configuration (may contain placeholders like ${today})</param>
    /// <param name="context">Execution context providing runtime information</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Dictionary with all placeholders resolved to concrete values</returns>
    Task<Dictionary<string, object>> ResolveAsync(
        Dictionary<string, object>? parameters,
        ParameterResolutionContext context,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Context information available during parameter resolution.
/// </summary>
public class ParameterResolutionContext
{
    /// <summary>
    /// The dataset being executed.
    /// </summary>
    public string DatasetId { get; set; } = string.Empty;

    /// <summary>
    /// The scheduled/actual execution time of the job.
    /// </summary>
    public DateTime ExecutionTime { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Current environment (Development, Staging, Production).
    /// </summary>
    public string? Environment { get; set; }

    /// <summary>
    /// Optional additional context values that can be referenced via ${context:key}.
    /// </summary>
    public Dictionary<string, object> AdditionalContext { get; set; } = new();
}
