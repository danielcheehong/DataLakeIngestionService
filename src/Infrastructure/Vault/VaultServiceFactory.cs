using DataLakeIngestionService.Core.Interfaces.Vault;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Vault;

public class VaultServiceFactory
{
    private readonly IConfiguration _configuration;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILoggerFactory _loggerFactory;

    public VaultServiceFactory(
        IConfiguration configuration,
        IHttpClientFactory httpClientFactory,
        ILoggerFactory loggerFactory)
    {
        _configuration = configuration;
        _httpClientFactory = httpClientFactory;
        _loggerFactory = loggerFactory;
    }

    public IVaultService Create()
    {
        var provider = _configuration["VaultConfiguration:Provider"] 
            ?? throw new InvalidOperationException("Vault provider not configured");

        return provider.ToLowerInvariant() switch
        {
            "securitas" => new SecuritasVaultService(
                _httpClientFactory.CreateClient("VaultClient"),
                _configuration,
                _loggerFactory.CreateLogger<SecuritasVaultService>()),
            
            "eva" => new EvaVaultService(
                _httpClientFactory.CreateClient("VaultClient"),
                _configuration,
                _loggerFactory.CreateLogger<EvaVaultService>()),
            
            _ => throw new NotSupportedException($"Vault provider '{provider}' is not supported")
        };
    }
}
