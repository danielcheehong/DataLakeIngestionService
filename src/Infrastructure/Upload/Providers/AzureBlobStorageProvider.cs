using Azure.Storage.Blobs;
using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Upload;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Upload.Providers;

public class AzureBlobStorageProvider : IUploadProvider
{
    private readonly ILogger<AzureBlobStorageProvider> _logger;
    private readonly AzureBlobOptions _options;

    public AzureBlobStorageProvider(
        ILogger<AzureBlobStorageProvider> logger,
        AzureBlobOptions options)
    {
        _logger = logger;
        _options = options;
    }

    public async Task<IUploadResult> UploadAsync(
        byte[] data,
        string destinationPath,
        string fileName,
        CancellationToken cancellationToken)
    {
        try
        {
            var blobServiceClient = new BlobServiceClient(_options.ConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(_options.ContainerName);

            // Ensure container exists
            await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

            var blobPath = Path.Combine(destinationPath, fileName).Replace('\\', '/');
            var blobClient = containerClient.GetBlobClient(blobPath);

            using var stream = new MemoryStream(data);
            await blobClient.UploadAsync(stream, overwrite: true, cancellationToken: cancellationToken);

            _logger.LogInformation("Uploaded {BytesWritten} bytes to Azure Blob: {BlobPath}",
                data.Length, blobClient.Uri);

            return new UploadResult
            {
                Success = true,
                Path = blobClient.Uri.ToString(),
                BytesWritten = data.Length
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to upload to Azure Blob Storage");
            throw new UploadException($"Azure Blob upload failed: {ex.Message}", ex);
        }
    }
}

public class AzureBlobOptions
{
    public string ConnectionString { get; set; } = string.Empty;
    public string ContainerName { get; set; } = string.Empty;
}
