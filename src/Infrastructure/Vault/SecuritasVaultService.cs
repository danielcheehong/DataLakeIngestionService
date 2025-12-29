using System;
using System.Net.Http.Headers;
using System.Text.Json;
using DataLakeIngestionService.Core.Interfaces.Vault;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Vault;

public class SecuritasVaultService : IVaultService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<SecuritasVaultService> _logger;
    private readonly string _baseUrl;
    private readonly string _authToken;

    public string ProviderName => "Securitas";

    public SecuritasVaultService(
        HttpClient httpClient,
        IConfiguration configuration,
        ILogger<SecuritasVaultService> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
        
        _baseUrl = configuration["VaultConfiguration:BaseUrl"] 
            ?? throw new InvalidOperationException("Vault BaseUrl not configured");
        
        _authToken = configuration["VaultConfiguration:AuthToken"] 
            ?? throw new InvalidOperationException("Vault AuthToken not configured");
    }

    public async Task<string> GetSecretAsync(string secretPath, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Retrieving secret from Securitas vault: {Path}", secretPath);

            var requestUrl = $"{_baseUrl}/v1/secret/data/{secretPath}";

            var request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _authToken);

            var response = await _httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync(cancellationToken);
            var secretData = JsonDocument.Parse(content);

            // Parse Securitas vault response format
            var secret = secretData.RootElement
                .GetProperty("data")
                .GetProperty("data")
                .GetProperty("value")
                .GetString();

            if (string.IsNullOrWhiteSpace(secret))
            {
                throw new InvalidOperationException($"Secret value is empty for path: {secretPath}");
            }

            _logger.LogInformation("Successfully retrieved secret from Securitas vault");
            return secret;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve secret from Securitas vault: {Path}", secretPath);
            throw;
        }
    }
}
