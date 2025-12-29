using System;
using System.Text.Json;
using DataLakeIngestionService.Core.Interfaces.Vault;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Vault;

public class EvaVaultService : IVaultService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<EvaVaultService> _logger;
    private readonly string _baseUrl;
    private readonly string _apiKey;

    public string ProviderName => "EVA";

    public EvaVaultService(
        HttpClient httpClient,
        IConfiguration configuration,
        ILogger<EvaVaultService> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
        
        _baseUrl = configuration["VaultConfiguration:BaseUrl"] 
            ?? throw new InvalidOperationException("Vault BaseUrl not configured");
        
        _apiKey = configuration["VaultConfiguration:ApiKey"] 
            ?? throw new InvalidOperationException("Vault ApiKey not configured");
    }

    public async Task<string> GetSecretAsync(string secretPath, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Retrieving secret from EVA vault: {Path}", secretPath);

            var requestUrl = $"{_baseUrl}/api/secrets/{secretPath}";

            var request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
            request.Headers.Add("X-API-Key", _apiKey);

            var response = await _httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync(cancellationToken);
            var secretData = JsonDocument.Parse(content);

            // Parse EVA vault response format
            var secret = secretData.RootElement
                .GetProperty("secret")
                .GetProperty("value")
                .GetString();

            if (string.IsNullOrWhiteSpace(secret))
            {
                throw new InvalidOperationException($"Secret value is empty for path: {secretPath}");
            }

            _logger.LogInformation("Successfully retrieved secret from EVA vault");
            return secret;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve secret from EVA vault: {Path}", secretPath);
            throw;
        }
    }
}
