namespace DataLakeIngestionService.Core.Interfaces.Upload;

public interface IUploadProvider
{
    Task<IUploadResult> UploadAsync(
        byte[] data,
        string destinationPath,
        string fileName,
        CancellationToken cancellationToken);
}

public interface IUploadProviderFactory
{
    IUploadProvider Create(string providerType);
}

public interface IUploadResult
{
    bool Success { get; set; }
    string Path { get; set; }
    long BytesWritten { get; set; }
}

public class UploadResult : IUploadResult
{
    public bool Success { get; set; }
    public string Path { get; set; } = string.Empty;
    public long BytesWritten { get; set; }
}
