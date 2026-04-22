namespace DataLakeIngestionService.Core.Interfaces.DataExtraction;

/// <summary>
/// Provides in-memory parameter overrides intended for test environments.
/// When an override exists for a parameter name, it takes full precedence over
/// the value defined in the dataset JSON configuration, bypassing all placeholder
/// resolution (e.g. ${today-1}).
/// Overrides are global (apply to all datasets) and are not persisted across restarts.
/// </summary>
public interface IParameterOverrideService
{
    /// <summary>
    /// Attempts to retrieve the override value for the given parameter name.
    /// Key comparison is case-insensitive.
    /// </summary>
    /// <returns>True if an override exists; false otherwise.</returns>
    bool TryGetOverride(string paramName, out object? value);

    /// <summary>
    /// Sets or replaces the override value for the given parameter name.
    /// </summary>
    void SetOverride(string paramName, object value);

    /// <summary>
    /// Removes the override for the given parameter name. No-op if not present.
    /// </summary>
    void RemoveOverride(string paramName);

    /// <summary>
    /// Removes all active overrides.
    /// </summary>
    void ClearAllOverrides();

    /// <summary>
    /// Returns a snapshot of all currently active overrides.
    /// </summary>
    IReadOnlyDictionary<string, object> GetAllOverrides();
}
