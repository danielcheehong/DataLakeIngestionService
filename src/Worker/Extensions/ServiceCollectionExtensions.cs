using System.Security.Cryptography.X509Certificates;
using DataLakeIngestionService.Core.Handlers;
using DataLakeIngestionService.Core.Interfaces.Certificate;
using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using DataLakeIngestionService.Core.Interfaces.Parquet;
using DataLakeIngestionService.Core.Interfaces.Services;
using DataLakeIngestionService.Core.Interfaces.Transformation;
using DataLakeIngestionService.Core.Interfaces.Upload;
using DataLakeIngestionService.Core.Interfaces.Vault;
using DataLakeIngestionService.Core.Pipeline;
using DataLakeIngestionService.Infrastructure.Certificate;
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
        services.AddScoped<CtlGenerationHandler>();
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
            var ctlHandler = sp.GetRequiredService<CtlGenerationHandler>();
            var uploadHandler = sp.GetRequiredService<UploadHandler>();

            return new DataPipeline(logger, extractionHandler, transformationHandler, parquetHandler, ctlHandler, uploadHandler);
        });
        

        // Register Infrastructure Services
        services.AddScoped<IDataSourceFactory, DataSourceFactory>();
        services.AddSingleton<ITransformationStepFactory, TransformationStepFactory>();
        services.AddScoped<ITransformationEngine, TransformationEngine>();
        services.AddScoped<IParquetWriter, ParquetWriterService>();
        services.AddScoped<ICtlWriter, CtlWriterService>();
        services.AddScoped<IUploadProviderFactory, UploadProviderFactory>();

        // Register Certificate Service
        services.Configure<CertificateServiceOptions>(
            configuration.GetSection("CertificateService"));
        services.AddSingleton<ICertificateService, LocalCertificateService>();

        // Register HttpClient for vault services with optional certificate auth
        services.AddHttpClient("VaultClient")
            .ConfigureHttpClient(client =>
            {
                client.Timeout = TimeSpan.FromSeconds(30);
            })
            .ConfigurePrimaryHttpMessageHandler(sp =>
            {
                var handler = new HttpClientHandler();

                var useCertAuth = configuration.GetValue<bool>("VaultConfiguration:UseCertificateAuth");

                if (useCertAuth)
                {
                    var certificate = GetVaultCertificate(sp, configuration);

                    if (certificate != null)
                    {
                        handler.ClientCertificates.Add(certificate);

                        var logger = sp.GetRequiredService<ILoggerFactory>()
                            .CreateLogger("VaultHttpClientSetup");

                        logger.LogInformation(
                            "Configured vault client with certificate: Subject={Subject}, Thumbprint={Thumbprint}, Expires={NotAfter}",
                            certificate.Subject,
                            certificate.Thumbprint,
                            certificate.NotAfter);

                        // Warn if certificate is expiring soon
                        if (certificate.NotAfter < DateTime.UtcNow.AddDays(30))
                        {
                            logger.LogWarning(
                                "Vault client certificate expires in {Days} days!",
                                (certificate.NotAfter - DateTime.UtcNow).Days);
                        }
                    }
                }

                return handler;
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

    /// <summary>
    /// Retrieves the vault client certificate from the local certificate store.
    /// </summary>
    private static X509Certificate2? GetVaultCertificate(
        IServiceProvider sp,
        IConfiguration configuration)
    {
        var certService = sp.GetRequiredService<ICertificateService>();
        var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
        var logger = loggerFactory.CreateLogger("VaultCertificateSetup");

        var thumbprint = configuration["VaultConfiguration:CertificateThumbprint"];
        var subjectName = configuration["VaultConfiguration:CertificateSubjectName"];

        var storeName = configuration.GetValue<StoreName?>("VaultConfiguration:CertificateStoreName")
            ?? StoreName.My;
        var storeLocation = configuration.GetValue<StoreLocation?>("VaultConfiguration:CertificateStoreLocation")
            ?? StoreLocation.LocalMachine;

        X509Certificate2? certificate = null;

        // Try thumbprint first
        if (!string.IsNullOrWhiteSpace(thumbprint))
        {
            logger.LogDebug("Looking for vault certificate by thumbprint: {Thumbprint}", thumbprint);
            certificate = certService.FindByThumbprint(thumbprint, storeName, storeLocation);
        }
        // Fall back to subject name
        else if (!string.IsNullOrWhiteSpace(subjectName))
        {
            logger.LogDebug("Looking for vault certificate by subject name: {SubjectName}", subjectName);
            certificate = certService.FindBySubjectName(subjectName, storeName, storeLocation);
        }

        if (certificate == null)
        {
            logger.LogError(
                "Vault certificate not found. Thumbprint={Thumbprint}, SubjectName={SubjectName}, Store={StoreName}/{StoreLocation}",
                thumbprint, subjectName, storeName, storeLocation);

            throw new InvalidOperationException(
                $"Vault certificate not found. Configure 'CertificateThumbprint' or 'CertificateSubjectName' in VaultConfiguration, " +
                $"and ensure the certificate is installed in {storeName}/{storeLocation} store.");
        }

        return certificate;
    }
}
