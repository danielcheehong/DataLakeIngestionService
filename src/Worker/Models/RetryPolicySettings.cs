namespace DataLakeIngestionService.Worker.Models;

public class RetryPolicySettings
{
    public int MaxRetries { get; set; } = 3;
    public double InitialDelaySeconds { get; set; } = 5;
    public bool UseJitter { get; set; } = true;
}
