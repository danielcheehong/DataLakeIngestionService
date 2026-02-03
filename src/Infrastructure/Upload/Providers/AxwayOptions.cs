namespace DataLakeIngestionService.Infrastructure.Upload.Providers;

public class AxwayOptions
{
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; } = 22;
    public string Username { get; set; } = string.Empty;
    public string PrivateKeyPath { get; set; } = string.Empty;
    public string PrivateKeyPassphrase { get; set; } = string.Empty;
    public string BasePath { get; set; } = string.Empty;
    public int ConnectionTimeoutSeconds { get; set; } = 30;
    public int MaxRetries { get; set; } = 3;
}
