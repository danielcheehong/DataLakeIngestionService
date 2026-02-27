using System.Text.Json;

namespace DataLakeIngestionService.Client;

class Program
{
    static async Task Main(string[] args)
    {
        var commandDir = "C:/DataLakeIngestionService/commands";
        Console.WriteLine("Enter command (e.g., jobs):");
        var command = Console.ReadLine()?.Trim();
        if (string.IsNullOrEmpty(command))
        {
            Console.WriteLine("No command entered.");
            return;
        }

        var cmdId = Guid.NewGuid().ToString("N");
        var cmdFile = Path.Combine(commandDir, $"{command}_{cmdId}.cmd");
        var resultFile = Path.ChangeExtension(cmdFile, ".result");

        var cmdObj = new { Command = command };
        var json = JsonSerializer.Serialize(cmdObj);
        Directory.CreateDirectory(commandDir);
        await File.WriteAllTextAsync(cmdFile, json);
        Console.WriteLine($"Command file written: {cmdFile}");

        Console.WriteLine("Waiting for result...");
        var timeout = TimeSpan.FromSeconds(30);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (File.Exists(resultFile))
            {
                var resultJson = await File.ReadAllTextAsync(resultFile);
                Console.WriteLine("Result:");
                Console.WriteLine(resultJson);
                File.Delete(resultFile);
                return;
            }
            await Task.Delay(500);
        }
        Console.WriteLine("Timeout waiting for result.");
    }
}
