using System.Data;
using DataColumn = System.Data.DataColumn;
using DataLakeIngestionService.Core.Interfaces.Parquet;
using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using ParquetColumn = Parquet.Data.DataColumn;

namespace DataLakeIngestionService.Infrastructure.Parquet;

public class ParquetWriterService : IParquetWriter
{
    private readonly ILogger<ParquetWriterService> _logger;

    public ParquetWriterService(ILogger<ParquetWriterService> logger)
    {
        _logger = logger;
    }

    public async Task WriteAsync(DataTable data, Stream outputStream, CancellationToken cancellationToken)
    {
        try
        {
            // Build Parquet schema from DataTable
            var fields = data.Columns.Cast<DataColumn>()
                .Select(col => new DataField(col.ColumnName, GetParquetType(col.DataType)))
                .ToArray();

            var schema = new ParquetSchema(fields);

            // Create Parquet writer
            using var writer = await ParquetWriter.CreateAsync(schema, outputStream);
            writer.CompressionMethod = CompressionMethod.Snappy;

            // Create row group
            using var rowGroup = writer.CreateRowGroup();

            // Write each column
            foreach (DataColumn column in data.Columns)
            {
                var columnData = data.AsEnumerable()
                    .Select(row => row[column])
                    .ToArray();

                await rowGroup.WriteColumnAsync(new ParquetColumn(
                    new DataField(column.ColumnName, GetParquetType(column.DataType)),
                    columnData));
            }

            _logger.LogInformation("Successfully wrote {RowCount} rows to Parquet", data.Rows.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write Parquet file");
            throw;
        }
    }

    private static Type GetParquetType(Type dataType)
    {
        return dataType.Name switch
        {
            nameof(Int32) => typeof(int),
            nameof(Int64) => typeof(long),
            nameof(String) => typeof(string),
            nameof(DateTime) => typeof(DateTimeOffset),
            nameof(Boolean) => typeof(bool),
            nameof(Decimal) => typeof(decimal),
            nameof(Double) => typeof(double),
            nameof(Single) => typeof(float),
            _ => typeof(string) // Default to string
        };
    }
}
