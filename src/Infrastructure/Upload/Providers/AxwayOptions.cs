namespace DataLakeIngestionService.Infrastructure.Upload.Providers;

public class AxwayOptions
{
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; } = 22;
    public string Username { get; set; } = string.Empty;
    public string PrivateKeyPath { get; set; } = string.Empty;
    public string PrivateKeyPassphrase { get; set; } = string.Empty;
    public string BasePath { get; set; } = "/";
    public int ConnectionTimeoutSeconds { get; set; } = 30;
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Maximum number of concurrent Axway SFTP uploads allowed.
    /// </summary>
    public int MaxConcurrentConnections { get; set; } = 2;
}
