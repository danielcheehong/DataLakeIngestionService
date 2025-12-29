using DataLakeIngestionService.Core.Handlers;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using DataLakeIngestionService.Core.Interfaces.Parquet;
using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using DataLakeIngestionService.Core.Interfaces.Upload;
using DataLakeIngestionService.Core.Interfaces.Vault;
using DataLakeIngestionService.Core.Pipeline;
using DataLakeIngestionService.Infrastructure.DataExtraction;
using DataLakeIngestionService.Infrastructure.Parquet;
using DataLakeIngestionService.Infrastructure.Services;
using DataLakeIngestionService.Infrastructure.Transformation;
using DataLakeIngestionService.Infrastructure.Upload;
using DataLakeIngestionService.Infrastructure.Upload.Providers;
using DataLakeIngestionService.Infrastructure.Vault;
using DataLakeIngestionService.Worker.Jobs;
using DataLakeIngestionService.Worker.Services;
using Quartz;

namespace DataLakeIngestionService.Worker.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationServices(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        // Register Core Pipeline
        services.AddScoped<ExtractionHandler>();
        services.AddScoped<TransformationHandler>();
        services.AddScoped<ParquetGenerationHandler>();
        services.AddScoped<UploadHandler>();

        // Register Data Pipeline for DataIngestionJob.
        // Note: DataPipeline is registered as Scoped to ensure a new instance per job execution.
        // Transformation steps can maintain state if needed in future enhancements, and be configured per dataset.
        services.AddScoped<DataPipeline>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<DataPipeline>>();
            var extractionHandler = sp.GetRequiredService<ExtractionHandler>();
            var transformationHandler = sp.GetRequiredService<TransformationHandler>();
            var parquetHandler = sp.GetRequiredService<ParquetGenerationHandler>();
            var uploadHandler = sp.GetRequiredService<UploadHandler>();

            return new DataPipeline(logger, extractionHandler, transformationHandler, parquetHandler, uploadHandler);
        });
        

        // Register Infrastructure Services
        services.AddScoped<IDataSourceFactory, DataSourceFactory>();
        services.AddSingleton<ITransformationStepFactory, TransformationStepFactory>();
        services.AddScoped<ITransformationEngine, TransformationEngine>();
        services.AddScoped<IParquetWriter, ParquetWriterService>();
        services.AddScoped<IUploadProviderFactory, UploadProviderFactory>();

        // Register HttpClient for vault services
        services.AddHttpClient("VaultClient")
            .ConfigureHttpClient(client =>
            {
                client.Timeout = TimeSpan.FromSeconds(30);
            });

        // Register Memory Cache for password caching
        services.AddMemoryCache();

        // Register Vault Service based on configuration
        services.AddSingleton<IVaultService>(sp =>
        {
            var config = sp.GetRequiredService<IConfiguration>();
            var httpClientFactory = sp.GetRequiredService<IHttpClientFactory>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            
            var factory = new VaultServiceFactory(config, httpClientFactory, loggerFactory);
            return factory.Create();
        });

        // Register Connection String Builder
        services.AddScoped<IConnectionStringBuilder, ConnectionStringBuilder>();

        // Register Configuration Service
        var datasetsPath = configuration.GetValue<string>("Datasets:ConfigurationPath") ?? "./Datasets";
        services.AddSingleton<IDatasetConfigurationService>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<DatasetConfigurationService>>();
            return new DatasetConfigurationService(logger, datasetsPath);
        });

        // Register Upload Provider Options
        var fileSystemBasePath = configuration.GetValue<string>("FileSystemProvider:BasePath") 
            ?? (OperatingSystem.IsWindows() ? "C:\\DataLake\\Output" : "/var/datalake/output");
        
        services.AddSingleton(new FileSystemOptions
        {
            BasePath = fileSystemBasePath,
            MaxRetries = configuration.GetValue<int>("FileSystemProvider:MaxRetries", 3)
        });

        services.AddSingleton(new AzureBlobOptions
        {
            ConnectionString = configuration.GetValue<string>("AzureBlob:ConnectionString") ?? string.Empty,
            ContainerName = configuration.GetValue<string>("AzureBlob:DefaultContainer") ?? "raw-data"
        });

        // Register Quartz.NET
        services.AddQuartz(q =>
        {
            // Use in-memory store for now
            q.UseInMemoryStore();

            // TODO: For production, use persistent store
            // q.UsePersistentStore(x =>
            // {
            //     x.UseSqlServer(configuration.GetConnectionString("QuartzDb"));
            //     x.UseJsonSerializer();
            // });
        });

        services.AddQuartzHostedService(options =>
        {
            options.WaitForJobsToComplete = true;
            options.AwaitApplicationStarted = true;
        });

        // Register Job Scheduling Service
        services.AddHostedService<JobSchedulingService>();

        return services;
    }
}
