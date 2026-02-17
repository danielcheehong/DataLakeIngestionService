using System.Data;

namespace DataLakeIngestionService.Core.Utilities;

/// <summary>
/// Static helper for DataTable schema operations, particularly for harmonizing
/// schemas across different data sources (e.g., Oracle and SQL Server).
/// </summary>
public static class DataTableSchemaHelper
{
    /// <summary>
    /// Creates a new DataTable with a harmonized schema based on the template.
    /// String columns have MaxLength set to -1 (unlimited) to accommodate values from any source.
    /// </summary>
    /// <param name="template">The DataTable to use as a schema template.</param>
    /// <returns>A new empty DataTable with relaxed string column constraints.</returns>
    public static DataTable CreateHarmonizedSchema(DataTable template)
    {
        if (template == null)
            throw new ArgumentNullException(nameof(template));

        var result = new DataTable(template.TableName);

        foreach (DataColumn col in template.Columns)
        {
            var newCol = CreateHarmonizedColumn(col.ColumnName, col.DataType, col.AllowDBNull);
            result.Columns.Add(newCol);
        }

        return result;
    }

    /// <summary>
    /// Creates a new DataTable with a harmonized schema from a collection of column definitions.
    /// Useful for join operations where columns come from multiple source tables.
    /// </summary>
    /// <param name="columns">Column definitions as tuples of (name, dataType, allowDBNull).</param>
    /// <returns>A new empty DataTable with relaxed string column constraints.</returns>
    public static DataTable CreateHarmonizedSchema(IEnumerable<(string Name, Type DataType, bool AllowDBNull)> columns)
    {
        if (columns == null)
            throw new ArgumentNullException(nameof(columns));

        var result = new DataTable();

        foreach (var (name, dataType, allowDBNull) in columns)
        {
            var newCol = CreateHarmonizedColumn(name, dataType, allowDBNull);
            result.Columns.Add(newCol);
        }

        return result;
    }

    /// <summary>
    /// Creates a single harmonized DataColumn with relaxed constraints.
    /// String columns have MaxLength = -1 (unlimited).
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <param name="dataType">The column data type.</param>
    /// <param name="allowDBNull">Whether the column allows null values (defaults to true for flexibility).</param>
    /// <returns>A new DataColumn with harmonized constraints.</returns>
    public static DataColumn CreateHarmonizedColumn(string columnName, Type dataType, bool allowDBNull = true)
    {
        var column = new DataColumn(columnName, dataType)
        {
            AllowDBNull = allowDBNull
        };

        // Set MaxLength to unlimited for string columns to avoid size mismatch issues
        // when merging data from different sources (e.g., Oracle VARCHAR2(50) vs SQL Server NVARCHAR(MAX))
        if (dataType == typeof(string))
        {
            column.MaxLength = -1;
        }

        return column;
    }
}
