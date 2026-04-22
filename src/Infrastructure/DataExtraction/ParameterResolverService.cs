using System.Text.RegularExpressions;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.DataExtraction;

/// <summary>
/// Resolves parameter placeholders like ${today}, ${today-1}, ${env:VAR} to runtime values.
/// 
/// Supported placeholder expressions:
/// - ${today}           - Today's date (yyyy-MM-dd)
/// - ${today-N}         - N days ago
/// - ${today+N}         - N days in future
/// - ${today-Nd}        - N days ago (explicit)
/// - ${today-Nw}        - N weeks ago
/// - ${today-Nm}        - N months ago
/// - ${today:format}    - Today with custom format (e.g., ${today:yyyyMMdd})
/// - ${now}             - Current datetime (yyyy-MM-dd HH:mm:ss)
/// - ${now-Nh}          - N hours ago
/// - ${now-Nm}          - N minutes ago
/// - ${now:format}      - Current datetime with custom format
/// - ${env:VAR_NAME}    - Environment variable value
/// - ${context:key}     - Value from additional context dictionary
/// 
/// Null parameters with date-related names (containing "date", "refdate", "asof") 
/// will default to today's date.
/// </summary>
public partial class ParameterResolverService : IParameterResolverService
{
    private readonly ILogger<ParameterResolverService> _logger;
    private readonly IParameterOverrideService _parameterOverrideService;

    // Pattern matches ${expression} where expression can contain alphanumeric, underscore, colon, plus, minus
    [GeneratedRegex(@"\$\{([^}]+)\}", RegexOptions.Compiled)]
    private static partial Regex PlaceholderPattern();

    // Date arithmetic patterns
    [GeneratedRegex(@"^today([+-])(\d+)$", RegexOptions.Compiled)]
    private static partial Regex TodayArithmeticPattern();

    [GeneratedRegex(@"^today([+-])(\d+)([dwm])$", RegexOptions.Compiled)]
    private static partial Regex TodayUnitArithmeticPattern();

    [GeneratedRegex(@"^now([+-])(\d+)$", RegexOptions.Compiled)]
    private static partial Regex NowArithmeticPattern();

    [GeneratedRegex(@"^now([+-])(\d+)([hm])$", RegexOptions.Compiled)]
    private static partial Regex NowUnitArithmeticPattern();

    public ParameterResolverService(
        ILogger<ParameterResolverService> logger,
        IParameterOverrideService parameterOverrideService)
    {
        _logger = logger;
        _parameterOverrideService = parameterOverrideService;
    }

    public Task<Dictionary<string, object>> ResolveAsync(
        Dictionary<string, object>? parameters,
        ParameterResolutionContext context,
        CancellationToken cancellationToken = default)
    {
        if (parameters == null || parameters.Count == 0)
        {
            return Task.FromResult(new Dictionary<string, object>());
        }

        var resolved = new Dictionary<string, object>(parameters.Count);

        foreach (var kvp in parameters)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var resolvedValue = ResolveValue(kvp.Key, kvp.Value, context);
            resolved[kvp.Key] = resolvedValue;

            if (!Equals(kvp.Value, resolvedValue))
            {
                _logger.LogDebug(
                    "Resolved parameter '{Name}': '{Original}' → '{Resolved}'",
                    kvp.Key, kvp.Value, resolvedValue);
            }
        }

        return Task.FromResult(resolved);
    }

    private object ResolveValue(string paramName, object? value, ParameterResolutionContext context)
    {
        // Check in-memory overrides first. When an override exists it takes full precedence,
        // bypassing placeholder resolution (e.g. ${today-1}) for that parameter.
        if (_parameterOverrideService.TryGetOverride(paramName, out var overrideValue) && overrideValue != null)
        {
            _logger.LogDebug("Parameter '{Name}' resolved from in-memory override: '{Value}'", paramName, overrideValue);
            return overrideValue;
        }

        // Handle null - could default based on parameter name conventions
        if (value == null)
        {
            return ResolveNullParameter(paramName, context);
        }

        // Only process string values for placeholders
        if (value is not string stringValue)
        {
            return value;
        }

        // Empty string - return as-is
        if (string.IsNullOrWhiteSpace(stringValue))
        {
            return value;
        }

        // Check if the entire value is a single placeholder
        if (stringValue.StartsWith("${") && stringValue.EndsWith("}") && stringValue.Count(c => c == '{') == 1)
        {
            var expression = stringValue[2..^1]; // Remove ${ and }
            return ResolvePlaceholder(expression, context);
        }

        // Check for embedded placeholders in a larger string
        if (stringValue.Contains("${"))
        {
            return PlaceholderPattern().Replace(stringValue, match =>
            {
                var expression = match.Groups[1].Value;
                var resolved = ResolvePlaceholder(expression, context);
                return resolved?.ToString() ?? string.Empty;
            });
        }

        // No placeholders - return original value
        return value;
    }

    private object ResolvePlaceholder(string expression, ParameterResolutionContext context)
    {
        var trimmed = expression.Trim();
        var lowerTrimmed = trimmed.ToLowerInvariant();

        // Environment variable: ${env:VARIABLE_NAME}
        if (lowerTrimmed.StartsWith("env:"))
        {
            var varName = trimmed[4..]; // Keep original case for env var name
            var envValue = Environment.GetEnvironmentVariable(varName);
            
            if (envValue == null)
            {
                _logger.LogWarning("Environment variable '{VarName}' not found, using empty string", varName);
                return string.Empty;
            }
            
            return envValue;
        }

        // Context values: ${context:key}
        if (lowerTrimmed.StartsWith("context:"))
        {
            var key = trimmed[8..];
            if (context.AdditionalContext.TryGetValue(key, out var ctxValue))
            {
                return ctxValue;
            }
            
            _logger.LogWarning("Context key '{Key}' not found, using empty string", key);
            return string.Empty;
        }

        // Format expressions with colon: ${today:yyyyMMdd}, ${now:yyyy-MM-dd HH:mm:ss}
        if (trimmed.Contains(':'))
        {
            var colonIndex = trimmed.IndexOf(':');
            var baseExpr = trimmed[..colonIndex].ToLowerInvariant();
            var format = trimmed[(colonIndex + 1)..];

            if (baseExpr == "today" || baseExpr == "now")
            {
                return ResolveFormattedDateExpression(baseExpr, format, context.ExecutionTime);
            }
        }

        // Date expressions: ${today}, ${today-1}, ${today+7}
        if (lowerTrimmed.StartsWith("today"))
        {
            return ResolveDateExpression(lowerTrimmed, context.ExecutionTime.Date);
        }

        // DateTime expressions: ${now}, ${now-1} (hours)
        if (lowerTrimmed.StartsWith("now"))
        {
            return ResolveDateTimeExpression(lowerTrimmed, context.ExecutionTime);
        }

        // Unknown placeholder - return as-is with warning
        _logger.LogWarning("Unknown placeholder expression: '{Expression}', returning as literal", expression);
        return $"${{{expression}}}";
    }

    private object ResolveDateExpression(string expression, DateTime baseDate)
    {
        // ${today} → today's date
        if (expression == "today")
        {
            return baseDate.Date;
        }

        // ${today-N} or ${today+N} → date arithmetic (days)
        var match = TodayArithmeticPattern().Match(expression);
        if (match.Success)
        {
            var op = match.Groups[1].Value;
            var days = int.Parse(match.Groups[2].Value);
            var result = op == "+" ? baseDate.AddDays(days) : baseDate.AddDays(-days);
            return result.Date;
        }

        // ${today-Nd} days, ${today-Nw} weeks, ${today-Nm} months
        var unitMatch = TodayUnitArithmeticPattern().Match(expression);
        if (unitMatch.Success)
        {
            var op = unitMatch.Groups[1].Value;
            var amount = int.Parse(unitMatch.Groups[2].Value);
            var unit = unitMatch.Groups[3].Value;
            var multiplier = op == "+" ? 1 : -1;

            var result = unit switch
            {
                "d" => baseDate.AddDays(amount * multiplier),
                "w" => baseDate.AddDays(amount * 7 * multiplier),
                "m" => baseDate.AddMonths(amount * multiplier),
                _ => baseDate
            };
            return result.Date;
        }

        // Fallback
        return baseDate.Date;
    }

    private object ResolveDateTimeExpression(string expression, DateTime baseDateTime)
    {
        // ${now} → current datetime
        if (expression == "now")
        {
            return baseDateTime;
        }

        // ${now-N} or ${now+N} → datetime arithmetic (hours by default)
        var match = NowArithmeticPattern().Match(expression);
        if (match.Success)
        {
            var op = match.Groups[1].Value;
            var hours = int.Parse(match.Groups[2].Value);
            var result = op == "+" ? baseDateTime.AddHours(hours) : baseDateTime.AddHours(-hours);
            return result;
        }

        // ${now-Nh} hours, ${now-Nm} minutes
        var unitMatch = NowUnitArithmeticPattern().Match(expression);
        if (unitMatch.Success)
        {
            var op = unitMatch.Groups[1].Value;
            var amount = int.Parse(unitMatch.Groups[2].Value);
            var unit = unitMatch.Groups[3].Value;
            var multiplier = op == "+" ? 1 : -1;

            var result = unit switch
            {
                "h" => baseDateTime.AddHours(amount * multiplier),
                "m" => baseDateTime.AddMinutes(amount * multiplier),
                _ => baseDateTime
            };
            return result;
        }

        // Fallback
        return baseDateTime;
    }

    private object ResolveFormattedDateExpression(string baseExpr, string format, DateTime baseDateTime)
    {
        var date = baseExpr == "today" ? baseDateTime.Date : baseDateTime;

        try
        {
            return date.ToString(format);
        }
        catch (FormatException ex)
        {
            _logger.LogWarning(ex, "Invalid date format '{Format}', using ISO format", format);
            return date.ToString("yyyy-MM-dd");
        }
    }

    private object ResolveNullParameter(string paramName, ParameterResolutionContext context)
    {
        // Convention-based defaults for null values
        var lowerName = paramName.ToLowerInvariant();

        // Date-related parameter names default to today
        if (lowerName.Contains("date") || 
            lowerName.Contains("refdate") || 
            lowerName == "asof" || 
            lowerName.Contains("asofdate"))
        {
            _logger.LogDebug("Null parameter '{Name}' defaulting to today's date", paramName);
            return context.ExecutionTime.Date;
        }

        // Return DBNull for database compatibility
        return DBNull.Value;
    }
}
