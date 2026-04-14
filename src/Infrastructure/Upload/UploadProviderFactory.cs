using DataLakeIngestionService.Core.Interfaces.Upload;
using DataLakeIngestionService.Infrastructure.Upload.Providers;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Upload;

public class UploadProviderFactory : IUploadProviderFactory
{
    private readonly ILogger<FileSystemUploadProvider> _fileSystemLogger;
    private readonly ILogger<AzureBlobStorageProvider> _azureLogger;
    private readonly ILogger<AxwayUploadProvider> _axwayLogger;
    private readonly FileSystemOptions _fileSystemOptions;
    private readonly AzureBlobOptions _azureOptions;
    private readonly AxwayOptions _axwayOptions;
    private readonly bool _testMode;

    public UploadProviderFactory(
        ILogger<FileSystemUploadProvider> fileSystemLogger,
        ILogger<AzureBlobStorageProvider> azureLogger,
        ILogger<AxwayUploadProvider> axwayLogger,
        FileSystemOptions fileSystemOptions,
        AzureBlobOptions azureOptions,
        AxwayOptions axwayOptions,
        bool testMode)
    {
        _fileSystemLogger = fileSystemLogger;
        _azureLogger = azureLogger;
        _axwayLogger = axwayLogger;
        _fileSystemOptions = fileSystemOptions;
        _azureOptions = azureOptions;
        _axwayOptions = axwayOptions;
        _testMode = testMode;
    }

    public IUploadProvider Create(string providerType)
    {
        if (_testMode)
        {
            return new FileSystemUploadProvider(_fileSystemLogger, _fileSystemOptions);
        }

        return providerType.ToLowerInvariant() switch
        {
            "filesystem" => new FileSystemUploadProvider(_fileSystemLogger, _fileSystemOptions),
            "azureblob" => new AzureBlobStorageProvider(_azureLogger, _azureOptions),
            "axway" => new AxwayUploadProvider(_axwayLogger, _axwayOptions),
            _ => throw new ArgumentException($"Unsupported upload provider: {providerType}", nameof(providerType))
        };
    }
}
