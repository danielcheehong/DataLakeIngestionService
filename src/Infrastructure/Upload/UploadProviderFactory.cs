using DataLakeIngestionService.Core.Interfaces.Upload;
using DataLakeIngestionService.Infrastructure.Upload.Providers;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Upload;

public class UploadProviderFactory : IUploadProviderFactory
{
    private readonly ILogger<FileSystemUploadProvider> _fileSystemLogger;
    private readonly ILogger<AzureBlobStorageProvider> _azureLogger;
    private readonly FileSystemOptions _fileSystemOptions;
    private readonly AzureBlobOptions _azureOptions;

    public UploadProviderFactory(
        ILogger<FileSystemUploadProvider> fileSystemLogger,
        ILogger<AzureBlobStorageProvider> azureLogger,
        FileSystemOptions fileSystemOptions,
        AzureBlobOptions azureOptions)
    {
        _fileSystemLogger = fileSystemLogger;
        _azureLogger = azureLogger;
        _fileSystemOptions = fileSystemOptions;
        _azureOptions = azureOptions;
    }

    public IUploadProvider Create(string providerType)
    {
        return providerType.ToLowerInvariant() switch
        {
            "filesystem" => new FileSystemUploadProvider(_fileSystemLogger, _fileSystemOptions),
            "azureblob" => new AzureBlobStorageProvider(_azureLogger, _azureOptions),
            _ => throw new ArgumentException($"Unsupported upload provider: {providerType}", nameof(providerType))
        };
    }
}
