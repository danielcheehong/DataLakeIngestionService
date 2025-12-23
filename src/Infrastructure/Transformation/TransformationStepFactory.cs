using System.Reflection;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Transformation;

public class TransformationStepFactory : ITransformationStepFactory
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TransformationStepFactory> _logger;
    private readonly Dictionary<string, Type> _stepTypes;

    public TransformationStepFactory(
        IServiceProvider serviceProvider,
        ILogger<TransformationStepFactory> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        
        // Scan assemblies once at startup
        _stepTypes = DiscoverTransformationSteps();
        
        _logger.LogInformation(
            "Discovered {Count} transformation steps: {Steps}",
            _stepTypes.Count,
            string.Join(", ", _stepTypes.Keys));
    }

    public ITransformationStep Create(string stepName, Dictionary<string, object>? config = null)
    {
        if (string.IsNullOrWhiteSpace(stepName))
        {
            throw new ArgumentException("Step name cannot be empty", nameof(stepName));
        }

        if (!_stepTypes.TryGetValue(stepName, out var stepType))
        {
            throw new ArgumentException(
                $"Transformation step '{stepName}' not found. Available steps: {string.Join(", ", _stepTypes.Keys)}",
                nameof(stepName));
        }

        try
        {
            // ActivatorUtilities resolves constructor dependencies from DI
            // and passes config as additional parameter
            var instance = ActivatorUtilities.CreateInstance(
                _serviceProvider, 
                stepType, 
                config ?? new Dictionary<string, object>());
            
            _logger.LogDebug("Created transformation step: {StepName} ({TypeName})", 
                stepName, stepType.Name);
            
            return (ITransformationStep)instance;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Failed to create transformation step '{StepName}' of type {TypeName}", 
                stepName, stepType.FullName);
            throw;
        }
    }

    public IEnumerable<string> GetAvailableSteps()
    {
        return _stepTypes.Keys.OrderBy(k => k);
    }

    private Dictionary<string, Type> DiscoverTransformationSteps()
    {
        var stepTypes = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

        // Scan both Core and Infrastructure assemblies
        var assemblies = new[]
        {
            Assembly.GetExecutingAssembly(), // Infrastructure assembly
            typeof(ITransformationStep).Assembly // Core assembly
        };

        foreach (var assembly in assemblies)
        {
            try
            {
                var types = assembly.GetTypes()
                    .Where(t => typeof(ITransformationStep).IsAssignableFrom(t)
                             && t.IsClass
                             && !t.IsAbstract
                             && !t.IsInterface);

                foreach (var type in types)
                {
                    var className = type.Name;
                    
                    // Remove "Step" suffix for cleaner names
                    // "DataCleansingStep" -> "DataCleansing"
                    if (className.EndsWith("Step", StringComparison.OrdinalIgnoreCase))
                    {
                        className = className.Substring(0, className.Length - 4);
                    }

                    if (stepTypes.ContainsKey(className))
                    {
                        _logger.LogWarning(
                            "Duplicate transformation step name '{Name}' found. " +
                            "Using {NewType}, ignoring {ExistingType}",
                            className, type.FullName, stepTypes[className].FullName);
                        continue;
                    }

                    stepTypes[className] = type;
                    
                    _logger.LogDebug(
                        "Registered transformation step: '{ClassName}' -> {TypeName}", 
                        className, type.FullName);
                }
            }
            catch (ReflectionTypeLoadException ex)
            {
                _logger.LogWarning(ex, 
                    "Failed to load some types from assembly {Assembly}", 
                    assembly.FullName);
            }
        }

        return stepTypes;
    }
}
