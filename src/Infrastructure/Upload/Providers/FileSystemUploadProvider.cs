using DataLakeIngestionService.Core.Exceptions;
using DataLakeIngestionService.Core.Interfaces.Upload;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Upload.Providers;

/// <summary>
/// Cross-platform file system upload provider supporting Windows (UNC paths) and Linux (mounted shares)
/// </summary>
public class FileSystemUploadProvider : IUploadProvider
{
    private readonly ILogger<FileSystemUploadProvider> _logger;
    private readonly FileSystemOptions _options;

    public FileSystemUploadProvider(
        ILogger<FileSystemUploadProvider> logger,
        FileSystemOptions options)
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
            // Build full path (cross-platform aware)
            var fullPath = BuildFullPath(destinationPath, fileName);
            
            _logger.LogInformation("Uploading file to: {Path}", fullPath);

            // Ensure directory exists
            var directory = Path.GetDirectoryName(fullPath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
                _logger.LogInformation("Created directory: {Directory}", directory);
            }

            // Use atomic write pattern (temp file + move)
            await AtomicWriteAsync(data, fullPath, cancellationToken);

            _logger.LogInformation("Successfully uploaded {BytesWritten} bytes to {Path}", 
                data.Length, fullPath);

            return new UploadResult
            {
                Success = true,
                Path = fullPath,
                BytesWritten = data.Length
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to upload file to file system");
            throw new UploadException($"File system upload failed: {ex.Message}", ex);
        }
    }

    private string BuildFullPath(string relativePath, string fileName)
    {
        // Normalize path separators for current platform
        var normalizedRelativePath = relativePath.Replace('\\', Path.DirectorySeparatorChar)
                                                 .Replace('/', Path.DirectorySeparatorChar);

        var fullPath = Path.Combine(_options.BasePath, normalizedRelativePath, fileName);
        
        // Resolve any relative path components
        fullPath = Path.GetFullPath(fullPath);

        return fullPath;
    }

    private async Task AtomicWriteAsync(byte[] data, string destinationPath, CancellationToken cancellationToken)
    {
        var tempPath = $"{destinationPath}.tmp.{Guid.NewGuid():N}";

        try
        {
            // Write to temporary file
            await File.WriteAllBytesAsync(tempPath, data, cancellationToken);

            // Atomic move
            File.Move(tempPath, destinationPath, overwrite: true);
        }
        catch
        {
            // Clean up temp file on failure
            if (File.Exists(tempPath))
            {
                try
                {
                    File.Delete(tempPath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to clean up temp file: {TempPath}", tempPath);
                }
            }
            throw;
        }
    }
}

public class FileSystemOptions
{
    public string BasePath { get; set; } = string.Empty;
    public int MaxRetries { get; set; } = 3;
}
