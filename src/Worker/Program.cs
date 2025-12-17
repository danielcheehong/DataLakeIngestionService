using DataLakeIngestionService.Worker.Extensions;
using Serilog;

namespace DataLakeIngestionService.Worker;

public class Program
{
    public static async Task Main(string[] args)
    {
        // Configure Serilog first
        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.File("logs/datalake-.log", rollingInterval: RollingInterval.Day)
            .CreateBootstrapLogger();

        try
        {
            Log.Information("Starting Data Lake Ingestion Service");

            var builder = Host.CreateApplicationBuilder(args);

            // Configure Serilog from configuration
            builder.Services.AddSerilog((services, lc) => lc
                .ReadFrom.Configuration(builder.Configuration)
                .ReadFrom.Services(services)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .WriteTo.File("logs/datalake-.log", rollingInterval: RollingInterval.Day));

            // Add cross-platform service support
            // Windows: Run as Windows Service
            // Linux: Run as systemd daemon
            if (OperatingSystem.IsWindows())
            {
                builder.Services.AddWindowsService(options =>
                {
                    options.ServiceName = "DataLakeIngestionService";
                });
                Log.Information("Configured for Windows Service hosting");
            }
            else if (OperatingSystem.IsLinux())
            {
                builder.Services.AddSystemd();
                Log.Information("Configured for Linux systemd hosting");
            }

            // Register application services
            builder.Services.AddApplicationServices(builder.Configuration);

            // Build and run
            var host = builder.Build();

            Log.Information("Service configured successfully. Starting host...");
            await host.RunAsync();
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Application terminated unexpectedly");
            throw;
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }
}
