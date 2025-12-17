// Implement ITransformationStep interface
// Basic transformation step examples
using System.Data;
using DataLakeIngestionService.Core.Interfaces.Transformation;


namespace DataLakeIngestionService.Infrastructure.Transformation;

public class DataCleansingStep : ITransformationStep
{
    public string Name => "DataCleansing";

    public Task<DataTable> TransformAsync(DataTable data, CancellationToken cancellationToken)
    {
        // Implement data cleansing logic (trim whitespace, remove null bytes, etc.)
        foreach (DataRow row in data.Rows)
        {
            foreach (DataColumn column in data.Columns)
            {
                if (row[column] is string strValue)
                {
                    row[column] = strValue.Trim();
                }
            }
        }

        return Task.FromResult(data);
    }
}