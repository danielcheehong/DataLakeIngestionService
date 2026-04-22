using System.Collections.Concurrent;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;

namespace DataLakeIngestionService.Infrastructure.DataExtraction;

/// <summary>
/// Thread-safe in-memory store for parameter overrides.
/// Keys are matched case-insensitively (OrdinalIgnoreCase).
/// Registered as a singleton; overrides are lost on service restart.
/// </summary>
public class ParameterOverrideService : IParameterOverrideService
{
    private readonly ConcurrentDictionary<string, object> _overrides =
        new(StringComparer.OrdinalIgnoreCase);

    public bool TryGetOverride(string paramName, out object? value)
    {
        var found = _overrides.TryGetValue(paramName, out var result);
        value = result;
        return found;
    }

    public void SetOverride(string paramName, object value) =>
        _overrides[paramName] = value;

    public void RemoveOverride(string paramName) =>
        _overrides.TryRemove(paramName, out _);

    public void ClearAllOverrides() =>
        _overrides.Clear();

    public IReadOnlyDictionary<string, object> GetAllOverrides() =>
        _overrides.ToDictionary(kvp => kvp.Key, kvp => kvp.Value, StringComparer.OrdinalIgnoreCase);
}
