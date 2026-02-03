using DataLakeIngestionService.Worker.Endpoints;
using DataLakeIngestionService.Worker.Extensions;
using DataLakeIngestionService.Worker.Services;
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

            // Use WebApplicationBuilder to support both API and background services
            var builder = WebApplication.CreateBuilder(args);

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

            // Add API services
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen(options =>
            {
                options.SwaggerDoc("v1", new Microsoft.OpenApi.Models.OpenApiInfo
                {
                    Title = "Data Lake Ingestion Service API",
                    Version = "v1",
                    Description = "API for managing Quartz scheduled data ingestion jobs"
                });
            });

            // Register Job Management Service for API
            builder.Services.AddScoped<IJobManagementService, JobManagementService>();

            // Register application services (Quartz, handlers, etc.)
            builder.Services.AddApplicationServices(builder.Configuration);

            // Build the application
            var app = builder.Build();

            // Configure HTTP pipeline
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI(options =>
                {
                    options.SwaggerEndpoint("/swagger/v1/swagger.json", "Data Lake Ingestion API v1");
                    options.RoutePrefix = "swagger";
                });
            }

            // Map API endpoints
            app.MapJobEndpoints();

            // Health check endpoint
            app.MapGet("/health", () => Results.Ok(new { Status = "Healthy", Timestamp = DateTime.UtcNow }))
                .WithTags("Health")
                .WithName("HealthCheck");

            Log.Information("Service configured successfully. Starting host...");
            await app.RunAsync();
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
