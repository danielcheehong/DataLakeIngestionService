namespace DataLakeIngestionService.Core.Constants;

/// <summary>
/// Standard environment names for environment-aware transformation execution
/// Future enhancement: to be used in validation of transformation step configurations
/// </summary>
public static class EnvironmentNames
{
    public const string Development = "Development";
    public const string Staging = "Staging";
    public const string Production = "Production";
    public const string DR = "DR";
    
    /// <summary>
    /// All known environment names for validation (case-insensitive)
    /// </summary>
    public static readonly HashSet<string> ValidEnvironments = new(StringComparer.OrdinalIgnoreCase)
    {
        Development,
        Staging,
        Production,
        DR
    };
}
