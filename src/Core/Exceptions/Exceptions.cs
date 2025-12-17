namespace DataLakeIngestionService.Core.Exceptions;

public class PipelineException : Exception
{
    public PipelineException(string message) : base(message) { }
    public PipelineException(string message, Exception innerException) : base(message, innerException) { }
}

public class ExtractionException : PipelineException
{
    public ExtractionException(string message) : base(message) { }
    public ExtractionException(string message, Exception innerException) : base(message, innerException) { }
}

public class TransformationException : PipelineException
{
    public TransformationException(string message) : base(message) { }
    public TransformationException(string message, Exception innerException) : base(message, innerException) { }
}

public class ParquetGenerationException : PipelineException
{
    public ParquetGenerationException(string message) : base(message) { }
    public ParquetGenerationException(string message, Exception innerException) : base(message, innerException) { }
}

public class UploadException : PipelineException
{
    public UploadException(string message) : base(message) { }
    public UploadException(string message, Exception innerException) : base(message, innerException) { }
}

public class NetworkException : Exception
{
    public NetworkException(string message) : base(message) { }
    public NetworkException(string message, Exception innerException) : base(message, innerException) { }
}
