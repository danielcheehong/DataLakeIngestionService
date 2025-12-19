using System.Data;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Parquet;
using Microsoft.Extensions.Logging;
using Parquet;
using ParquetData = Parquet.Data;
using Parquet.Schema;

namespace DataLakeIngestionService.Infrastructure.Parquet;

public class ParquetWriterService : IParquetWriter
{
    private readonly ILogger<ParquetWriterService> _logger;

    public ParquetWriterService(ILogger<ParquetWriterService> logger)
    {
        _logger = logger;
    }

    public async Task WriteAsync(DataTable data, Stream outputStream, CancellationToken cancellationToken = default)
    {
        try
        {
            if (data.Rows.Count == 0)
            {
                _logger.LogWarning("No data to write to Parquet");
                return;
            }

            // Convert DateTimeOffset columns to DateTime
            ConvertDateTimeOffsetColumns(data);

                        // Create Parquet schema from DataTable columns
            var fields = data.Columns.Cast<System.Data.DataColumn>()
                .Select(col => new DataField(col.ColumnName, GetParquetType(col.DataType)))
                .ToArray();

            var schema = new ParquetSchema(fields);

            // Write data
            using var writer = await ParquetWriter.CreateAsync(schema, outputStream);
            writer.CompressionMethod = CompressionMethod.Snappy;

            using var groupWriter = writer.CreateRowGroup();

            // Write columns using fields from schema (must reuse same DataField instances)
            for (int i = 0; i < data.Columns.Count; i++)
            {
                var column = data.Columns[i];
                var schemaField = (DataField)schema.Fields[i]; // ✅ Cast to DataField to access ClrType

                _logger.LogDebug("Processing column '{Name}': Type={Type}, AllowDBNull={AllowNull}, ClrType={ClrType}",
                    column.ColumnName, column.DataType, column.AllowDBNull, schemaField.ClrType);

                // Create strongly-typed array
                var columnData = CreateTypedArrayForColumn(data, column, schemaField.ClrType);

                _logger.LogDebug("Created array of type {ArrayType} for column '{Name}'", 
                    columnData.GetType(), column.ColumnName);

                var dataColumn = new ParquetData.DataColumn(schemaField, columnData);

                await groupWriter.WriteColumnAsync(dataColumn, cancellationToken);
            }

            _logger.LogInformation("Successfully wrote {RowCount} rows to Parquet", data.Rows.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write Parquet file");
            throw new ParquetGenerationException($"Failed to generate Parquet file: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Converts DateTimeOffset columns to DateTime in the DataTable
    /// </summary>
    private void ConvertDateTimeOffsetColumns(DataTable dataTable)
    {
        var dateTimeOffsetColumns = dataTable.Columns.Cast<System.Data.DataColumn>()
            .Where(col => col.DataType == typeof(DateTimeOffset))
            .ToList();

        if (!dateTimeOffsetColumns.Any())
            return;

        _logger.LogInformation("Converting {Count} DateTimeOffset columns to DateTime", dateTimeOffsetColumns.Count);

        foreach (var column in dateTimeOffsetColumns)
        {
            var columnIndex = column.Ordinal;
            var columnName = column.ColumnName;

            // Create new DateTime column
            var newColumn = new System.Data.DataColumn(columnName + "_temp", typeof(DateTime))
            {
                AllowDBNull = column.AllowDBNull
            };
            dataTable.Columns.Add(newColumn);

            // Copy and convert values
            foreach (DataRow row in dataTable.Rows)
            {
                if (row[columnIndex] != DBNull.Value && row[columnIndex] is DateTimeOffset dateTimeOffset)
                {
                    row[newColumn] = dateTimeOffset.DateTime;
                }
                else
                {
                    row[newColumn] = DBNull.Value;
                }
            }

            // Remove old column and rename new one
            dataTable.Columns.Remove(column);
            newColumn.ColumnName = columnName;
        }
    }

    /// <summary>
    /// Maps .NET types to Parquet types
    /// </summary>
    private static Type GetParquetType(Type clrType)
    {
        // Handle nullable types
        var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

        // Convert DateTimeOffset to DateTime
        if (underlyingType == typeof(DateTimeOffset))
        {
            return typeof(DateTime);
        }

        return underlyingType switch
        {
            Type t when t == typeof(string) => typeof(string),
            Type t when t == typeof(int) => typeof(int),
            Type t when t == typeof(long) => typeof(long),
            Type t when t == typeof(short) => typeof(short),
            Type t when t == typeof(byte) => typeof(byte),
            Type t when t == typeof(bool) => typeof(bool),
            Type t when t == typeof(float) => typeof(float),
            Type t when t == typeof(double) => typeof(double),
            Type t when t == typeof(decimal) => typeof(decimal),
            Type t when t == typeof(DateTime) => typeof(DateTime),
            Type t when t == typeof(TimeSpan) => typeof(TimeSpan),
            Type t when t == typeof(Guid) => typeof(string), // Store GUID as string
            Type t when t == typeof(byte[]) => typeof(byte[]),
            _ => typeof(string) // Default to string for unknown types
        };
    }

    /// <summary>
    /// Converts a value to the appropriate type for Parquet
    /// </summary>
    private static object? ConvertValue(object value, Type targetType)
    {
        if (value == DBNull.Value || value == null)
            return null;

        // Convert DateTimeOffset to DateTime
        if (value is DateTimeOffset dateTimeOffset)
        {
            return dateTimeOffset.DateTime;
        }

        // Convert GUID to string
        if (value is Guid guid)
        {
            return guid.ToString();
        }

        return value;
    }

    /// <summary>
    /// Creates a strongly-typed array for Parquet column
    /// </summary>
    private static Array CreateTypedArrayForColumn(DataTable dataTable, System.Data.DataColumn column, Type targetType)
    {
        var rowCount = dataTable.Rows.Count;

        if (targetType == typeof(int))
        {
            var result = new int[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                var value = dataTable.Rows[i][column];
                result[i] = value == DBNull.Value ? 0 : Convert.ToInt32(value);
            }
            return result;
        }
        else if (targetType == typeof(long))
        {
            var result = new long[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                var value = dataTable.Rows[i][column];
                result[i] = value == DBNull.Value ? 0L : Convert.ToInt64(value);
            }
            return result;
        }
        else if (targetType == typeof(decimal))
        {
            var result = new decimal[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                var value = dataTable.Rows[i][column];
                result[i] = value == DBNull.Value ? 0m : Convert.ToDecimal(value);
            }
            return result;
        }
        else if (targetType == typeof(double))
        {
            var result = new double[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                var value = dataTable.Rows[i][column];
                result[i] = value == DBNull.Value ? 0.0 : Convert.ToDouble(value);
            }
            return result;
        }
        else if (targetType == typeof(bool))
        {
            var result = new bool[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                var value = dataTable.Rows[i][column];
                result[i] = value != DBNull.Value && Convert.ToBoolean(value);
            }
            return result;
        }
        else if (targetType == typeof(DateTime))
        {
            var result = new DateTime[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                var value = dataTable.Rows[i][column];
                if (value == DBNull.Value)
                {
                    result[i] = DateTime.MinValue;
                }
                else if (value is DateTimeOffset dto)
                {
                    result[i] = dto.DateTime;
                }
                else
                {
                    result[i] = Convert.ToDateTime(value);
                }
            }
            return result;
        }
        else if (targetType == typeof(string))
        {
            // Strings are always nullable in Parquet
            var result = new string?[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                var value = dataTable.Rows[i][column];
                if (value == DBNull.Value || value == null)
                {
                    result[i] = null;
                }
                else if (value is Guid guid)
                {
                    result[i] = guid.ToString();
                }
                else
                {
                    result[i] = value.ToString();
                }
            }
            return result;
        }
        else
        {
            // Default to string array (always nullable)
            var result = new string?[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                var value = dataTable.Rows[i][column];
                result[i] = value == DBNull.Value ? null : value?.ToString();
            }
            return result;
        }
    }
}