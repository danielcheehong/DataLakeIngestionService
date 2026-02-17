using System.Data;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using DataLakeIngestionService.Core.Pipeline;
using DataLakeIngestionService.Core.Utilities;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Transformation.Common;

/// <summary>
/// Transformation step that merges multiple DataTables from ExtractedDataSets into a single ExtractedData table.
/// This step should be configured as the final transformation for multi-source datasets.
/// </summary>
public class MergeTablesStep : ITransformationStep
{
    private readonly ILogger<MergeTablesStep> _logger;
    private readonly Dictionary<string, object> _config;

    public MergeTablesStep(
        ILogger<MergeTablesStep> logger,
        Dictionary<string, object>? config = null)
    {
        _logger = logger;
        _config = config ?? new Dictionary<string, object>();
    }

    public string Name => "MergeTables";
    
    public List<string> Environments { get; set; } = new();

    public Task TransformAsync(IPipelineContext context, CancellationToken cancellationToken)
    {
        var mergeType = GetConfigValue("mergeType", "Union");
        var joinKeys = GetConfigValue<string[]>("joinKeys", Array.Empty<string>());
        var sourceTables = GetConfigValue<string[]>("sourceTables", Array.Empty<string>());
        
        _logger.LogInformation(
            "Merging tables using {MergeType}. Sources: {SourceCount}, JoinKeys: [{JoinKeys}]",
            mergeType,
            context.ExtractedDataSets.Count,
            string.Join(", ", joinKeys));

        if (context.ExtractedDataSets.Count == 0)
        {
            _logger.LogWarning("No tables in ExtractedDataSets to merge");
            return Task.CompletedTask;
        }

        // Determine which tables to merge
        var tablesToMerge = sourceTables.Length > 0
            ? context.ExtractedDataSets
                .Where(kvp => sourceTables.Contains(kvp.Key, StringComparer.OrdinalIgnoreCase))
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
            : context.ExtractedDataSets.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

        if (tablesToMerge.Count == 0)
        {
            throw new TransformationException(
                $"No matching tables found for merge. Requested: [{string.Join(", ", sourceTables)}], " +
                $"Available: [{string.Join(", ", context.ExtractedDataSets.Keys)}]");
        }

        DataTable mergedTable = mergeType.ToLowerInvariant() switch
        {
            "union" => MergeUnion(tablesToMerge.Values.ToList(), cancellationToken),
            "innerjoin" => MergeJoin(tablesToMerge.Values.ToList(), joinKeys, JoinType.Inner, cancellationToken),
            "leftjoin" => MergeJoin(tablesToMerge.Values.ToList(), joinKeys, JoinType.Left, cancellationToken),
            _ => throw new TransformationException($"Unsupported merge type: {mergeType}")
        };

        context.ExtractedData = mergedTable;

        _logger.LogInformation(
            "Merged {SourceCount} tables into single table with {RowCount} rows",
            tablesToMerge.Count,
            mergedTable.Rows.Count);

        return Task.CompletedTask;
    }

    private DataTable MergeUnion(List<DataTable> tables, CancellationToken cancellationToken)
    {
        if (tables.Count == 0)
            return new DataTable();

        // Use harmonized schema to avoid column size mismatch between Oracle and SQL Server
        var result = DataTableSchemaHelper.CreateHarmonizedSchema(tables[0]);
        
        foreach (var table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            foreach (DataRow row in table.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                
                // Manual row copy instead of ImportRow to avoid schema constraint issues
                var newRow = result.NewRow();
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    newRow[i] = row[i];
                }
                result.Rows.Add(newRow);
            }
        }

        _logger.LogDebug("Union merge completed. Total rows: {RowCount}", result.Rows.Count);
        return result;
    }

    private DataTable MergeJoin(
        List<DataTable> tables, 
        string[] joinKeys, 
        JoinType joinType,
        CancellationToken cancellationToken)
    {
        if (tables.Count < 2)
        {
            throw new TransformationException("Join requires at least 2 tables");
        }

        if (joinKeys.Length == 0)
        {
            throw new TransformationException("Join requires at least one join key column");
        }

        // Start with first table
        var result = tables[0].Copy();

        // Join subsequent tables
        for (int i = 1; i < tables.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            result = JoinTwoTables(result, tables[i], joinKeys, joinType, cancellationToken);
        }

        _logger.LogDebug("{JoinType} join completed. Result rows: {RowCount}", joinType, result.Rows.Count);
        return result;
    }

    private DataTable JoinTwoTables(
        DataTable left, 
        DataTable right, 
        string[] joinKeys,
        JoinType joinType,
        CancellationToken cancellationToken)
    {
        // Build column definitions for harmonized schema
        var columnDefinitions = new List<(string Name, Type DataType, bool AllowDBNull)>();
        
        // Add columns from left table
        foreach (DataColumn col in left.Columns)
        {
            columnDefinitions.Add((col.ColumnName, col.DataType, col.AllowDBNull));
        }
        
        // Add columns from right table (skip join key columns to avoid duplicates)
        var rightColumnsToAdd = new List<DataColumn>();
        foreach (DataColumn col in right.Columns)
        {
            if (!joinKeys.Contains(col.ColumnName, StringComparer.OrdinalIgnoreCase))
            {
                var newColName = columnDefinitions.Any(c => c.Name.Equals(col.ColumnName, StringComparison.OrdinalIgnoreCase))
                    ? $"{col.ColumnName}_2" 
                    : col.ColumnName;
                columnDefinitions.Add((newColName, col.DataType, col.AllowDBNull));
                rightColumnsToAdd.Add(col);
            }
        }

        // Create result table with harmonized schema to avoid column size mismatch
        var result = DataTableSchemaHelper.CreateHarmonizedSchema(columnDefinitions);

        // Build index on right table for efficient lookup
        var rightIndex = new Dictionary<string, List<DataRow>>();
        foreach (DataRow row in right.Rows)
        {
            var key = BuildCompositeKey(row, joinKeys);
            if (!rightIndex.ContainsKey(key))
                rightIndex[key] = new List<DataRow>();
            rightIndex[key].Add(row);
        }

        // Perform join
        foreach (DataRow leftRow in left.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            var key = BuildCompositeKey(leftRow, joinKeys);
            
            if (rightIndex.TryGetValue(key, out var matchingRows))
            {
                foreach (var rightRow in matchingRows)
                {
                    var newRow = result.NewRow();
                    CopyRowValues(newRow, leftRow, rightColumnsToAdd, rightRow);
                    result.Rows.Add(newRow);
                }
            }
            else if (joinType == JoinType.Left)
            {
                // Left join: include left row even without match
                var newRow = result.NewRow();
                CopyLeftRowOnly(newRow, leftRow, rightColumnsToAdd.Count);
                result.Rows.Add(newRow);
            }
            // Inner join: skip rows without match
        }

        return result;
    }

    private string BuildCompositeKey(DataRow row, string[] keyColumns)
    {
        return string.Join("|", keyColumns.Select(col => row[col]?.ToString() ?? ""));
    }

    private void CopyRowValues(DataRow target, DataRow leftRow, List<DataColumn> rightColumns, DataRow rightRow)
    {
        int colIndex = 0;
        
        // Copy left row values
        foreach (DataColumn col in leftRow.Table.Columns)
        {
            target[colIndex++] = leftRow[col];
        }
        
        // Copy right row values (only non-join-key columns)
        foreach (var col in rightColumns)
        {
            target[colIndex++] = rightRow[col];
        }
    }

    private void CopyLeftRowOnly(DataRow target, DataRow leftRow, int rightColumnCount)
    {
        int colIndex = 0;
        
        // Copy left row values
        foreach (DataColumn col in leftRow.Table.Columns)
        {
            target[colIndex++] = leftRow[col];
        }
        
        // Set nulls for right columns
        for (int i = 0; i < rightColumnCount; i++)
        {
            target[colIndex++] = DBNull.Value;
        }
    }

    private T GetConfigValue<T>(string key, T defaultValue)
    {
        if (!_config.TryGetValue(key, out var value))
            return defaultValue;

        try
        {
            if (value is T typedValue)
                return typedValue;
            
            if (value is System.Text.Json.JsonElement jsonElement)
            {
                return System.Text.Json.JsonSerializer.Deserialize<T>(jsonElement.GetRawText())
                    ?? defaultValue;
            }
                
            return (T)Convert.ChangeType(value, typeof(T));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to convert config '{Key}' to {Type}, using default", 
                key, typeof(T).Name);
            return defaultValue;
        }
    }

    private enum JoinType
    {
        Inner,
        Left
    }
}
