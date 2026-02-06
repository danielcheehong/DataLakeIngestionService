using System.Reflection;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.DataExtraction;

public class DotNetDataProviderFactory : IDotNetDataProviderFactory
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<DotNetDataProviderFactory> _logger;
    private readonly Dictionary<string, Type> _providerTypes;

    public DotNetDataProviderFactory(
        IServiceProvider serviceProvider,
        ILogger<DotNetDataProviderFactory> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;

        // Scan assemblies once at startup
        _providerTypes = DiscoverProviders();

        _logger.LogInformation(
            "Discovered {Count} DotNet data providers: {Providers}",
            _providerTypes.Count,
            string.Join(", ", _providerTypes.Keys));
    }

    public IDotNetDataProvider Create(string providerName)
    {
        if (string.IsNullOrWhiteSpace(providerName))
        {
            throw new ArgumentException("Provider name cannot be empty", nameof(providerName));
        }

        if (!_providerTypes.TryGetValue(providerName, out var providerType))
        {
            throw new ArgumentException(
                $"DotNet data provider '{providerName}' not found. Available providers: {string.Join(", ", _providerTypes.Keys)}",
                nameof(providerName));
        }

        try
        {
            // ActivatorUtilities resolves constructor dependencies from DI
            var instance = ActivatorUtilities.CreateInstance(_serviceProvider, providerType);

            _logger.LogDebug("Created DotNet data provider: {ProviderName} ({TypeName})",
                providerName, providerType.Name);

            return (IDotNetDataProvider)instance;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to create DotNet data provider '{ProviderName}' of type {TypeName}",
                providerName, providerType.FullName);
            throw;
        }
    }

    public IEnumerable<string> GetAvailableProviders()
    {
        return _providerTypes.Keys.OrderBy(k => k);
    }

    private Dictionary<string, Type> DiscoverProviders()
    {
        var providerTypes = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

        // Scan Infrastructure assembly and any loaded assemblies that reference Core
        var assemblies = new List<Assembly>
        {
            Assembly.GetExecutingAssembly(), // Infrastructure assembly
            typeof(IDotNetDataProvider).Assembly // Core assembly
        };

        // Also scan the entry assembly (Worker) in case providers are defined there
        var entryAssembly = Assembly.GetEntryAssembly();
        if (entryAssembly != null && !assemblies.Contains(entryAssembly))
        {
            assemblies.Add(entryAssembly);
        }

        foreach (var assembly in assemblies)
        {
            try
            {
                var types = assembly.GetTypes()
                    .Where(t => typeof(IDotNetDataProvider).IsAssignableFrom(t)
                             && t.IsClass
                             && !t.IsAbstract
                             && !t.IsInterface);

                foreach (var type in types)
                {
                    // Use naming convention to derive provider name
                    var providerName = GetProviderName(type);

                    if (providerTypes.ContainsKey(providerName))
                    {
                        _logger.LogWarning(
                            "Duplicate DotNet data provider name '{Name}' found. " +
                            "Using {ExistingType}, ignoring {NewType}",
                            providerName, providerTypes[providerName].FullName, type.FullName);
                        continue;
                    }

                    providerTypes[providerName] = type;

                    _logger.LogDebug(
                        "Registered DotNet data provider: '{ProviderName}' -> {TypeName}",
                        providerName, type.FullName);
                }
            }
            catch (ReflectionTypeLoadException ex)
            {
                _logger.LogWarning(ex,
                    "Failed to load some types from assembly {Assembly}",
                    assembly.FullName);
            }
        }

        return providerTypes;
    }

    private static string GetProviderName(Type type)
    {
        var className = type.Name;

        // Remove "Provider" suffix for cleaner names
        // "SampleMetricsProvider" -> "SampleMetrics"
        if (className.EndsWith("Provider", StringComparison.OrdinalIgnoreCase))
        {
            className = className.Substring(0, className.Length - 8);
        }

        return className;
    }
}
