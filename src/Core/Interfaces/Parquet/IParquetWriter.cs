using System.Data;

namespace DataLakeIngestionService.Core.Interfaces.Parquet;

public interface IParquetWriter
{
    Task WriteAsync(DataTable data, Stream outputStream, CancellationToken cancellationToken);
}

public interface ISchemaBuilder
{
    object BuildSchema(DataTable dataTable);
}
