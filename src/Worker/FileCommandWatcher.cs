using System.Text.Json;
using DataLakeIngestionService.Worker.Services;

namespace DataLakeIngestionService.Worker;

public class FileCommandWatcher
{
    private readonly string _commandDir;
    private readonly IServiceProvider _serviceProvider;
    private readonly CancellationToken _cancellationToken;

    public FileCommandWatcher(string commandDir, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        _commandDir = commandDir;
        _serviceProvider = serviceProvider;
        _cancellationToken = cancellationToken;
    }

    public void Start()
    {
        Task.Run(async () => await WatchLoop());
    }

    private async Task WatchLoop()
    {
        Directory.CreateDirectory(_commandDir);
        while (!_cancellationToken.IsCancellationRequested)
        {
            var cmdFiles = Directory.GetFiles(_commandDir, "*.cmd");
            foreach (var cmdFile in cmdFiles)
            {
                try
                {
                    var cmdText = await File.ReadAllTextAsync(cmdFile, _cancellationToken);
                    var cmd = JsonSerializer.Deserialize<CommandRequest>(cmdText);
                    if (cmd != null)
                    {
                        await HandleCommand(cmd, cmdFile);
                    }
                }
                catch { /* log or ignore */ }
            }
            await Task.Delay(500, _cancellationToken);
        }
    }

    private async Task HandleCommand(CommandRequest cmd, string cmdFile)
    {
        var resultFile = Path.ChangeExtension(cmdFile, ".result");
        using var scope = _serviceProvider.CreateScope();
        var jobService = scope.ServiceProvider.GetRequiredService<IJobManagementService>();
        object? result = null;
        switch (cmd.Command?.ToLowerInvariant())
        {
            case "jobs":
                result = await jobService.GetAllJobsAsync(_cancellationToken);
                break;
            // Add more cases for other commands
            default:
                result = new { Error = "Unknown command" };
                break;
        }
        var json = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(resultFile, json, _cancellationToken);
        File.Delete(cmdFile);
    }

    public class CommandRequest
    {
        public string? Command { get; set; }
        // Add more properties for arguments if needed
    }
}
