using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Upload;
using Microsoft.Extensions.Logging;
using Renci.SshNet;

namespace DataLakeIngestionService.Infrastructure.Upload.Providers;

public class AxwayUploadProvider : IUploadProvider
{
    private readonly ILogger<AxwayUploadProvider> _logger;
    private readonly AxwayOptions _options;

    public AxwayUploadProvider(
        ILogger<AxwayUploadProvider> logger,
        AxwayOptions options)
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
        var fullPath = Path.Combine(_options.BasePath, destinationPath, fileName)
            .Replace("\\", "/"); // Ensure Unix-style paths for SFTP

        _logger.LogInformation("Uploading {FileName} to Axway SFTP: {Host}:{Path}",
            fileName, _options.Host, fullPath);

        try
        {
            var connectionInfo = CreateConnectionInfo();

            using var client = new SftpClient(connectionInfo);
            await Task.Run(() =>
            {
                client.Connect();

                // Ensure directory exists
                var directory = Path.GetDirectoryName(fullPath)?.Replace("\\", "/");
                if (!string.IsNullOrEmpty(directory))
                {
                    CreateDirectoryRecursive(client, directory);
                }

                using var stream = new MemoryStream(data);
                client.UploadFile(stream, fullPath, canOverride: true);

                client.Disconnect();
            }, cancellationToken);

            _logger.LogInformation("Successfully uploaded {FileName} ({Bytes} bytes) to Axway",
                fileName, data.Length);

            return new UploadResult
            {
                Success = true,
                Path = $"sftp://{_options.Host}{fullPath}",
                BytesWritten = data.Length
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to upload {FileName} to Axway SFTP", fileName);
            throw new UploadException($"Axway SFTP upload failed: {ex.Message}", ex);
        }
    }

    private ConnectionInfo CreateConnectionInfo()
    {
        var authMethods = new List<AuthenticationMethod>();

        if (!string.IsNullOrEmpty(_options.PrivateKeyPath))
        {
            var keyFile = string.IsNullOrEmpty(_options.PrivateKeyPassphrase)
                ? new PrivateKeyFile(_options.PrivateKeyPath)
                : new PrivateKeyFile(_options.PrivateKeyPath, _options.PrivateKeyPassphrase);

            authMethods.Add(new PrivateKeyAuthenticationMethod(_options.Username, keyFile));
        }

        return new ConnectionInfo(
            _options.Host,
            _options.Port,
            _options.Username,
            authMethods.ToArray())
        {
            Timeout = TimeSpan.FromSeconds(_options.ConnectionTimeoutSeconds)
        };
    }

    private static void CreateDirectoryRecursive(SftpClient client, string path)
    {
        var parts = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var current = "";

        foreach (var part in parts)
        {
            current = $"{current}/{part}";
            if (!client.Exists(current))
            {
                client.CreateDirectory(current);
            }
        }
    }
}
