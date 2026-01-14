using System.Net.Http.Headers;
using System.Text.Json;
using DataLakeIngestionService.Core.Interfaces.Vault;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataLakeIngestionService.Infrastructure.Vault;

/// <summary>
/// Vault service that retrieves secrets from Securitas vault.
/// Supports both bearer token and client certificate (mTLS) authentication.
/// </summary>
public class SecuritasVaultService : IVaultService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<SecuritasVaultService> _logger;
    private readonly string _baseUrl;
    private readonly string? _authToken;
    private readonly bool _useBearerToken;

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

        // Certificate auth may be used alone or with bearer token
        var useCertAuth = configuration.GetValue<bool>("VaultConfiguration:UseCertificateAuth");
        _useBearerToken = configuration.GetValue<bool>("VaultConfiguration:UseBearerTokenWithCertificate", true);

        // Auth token is optional when using certificate-only auth
        _authToken = configuration["VaultConfiguration:AuthToken"];

        if (!useCertAuth && string.IsNullOrWhiteSpace(_authToken))
        {
            throw new InvalidOperationException(
                "Vault AuthToken must be configured when UseCertificateAuth is false");
        }

        if (useCertAuth)
        {
            _logger.LogInformation(
                "SecuritasVaultService configured with certificate authentication. BearerToken={UseBearerToken}",
                _useBearerToken && !string.IsNullOrWhiteSpace(_authToken));
        }
        else
        {
            _logger.LogInformation("SecuritasVaultService configured with bearer token authentication");
        }
    }

    public async Task<string> GetSecretAsync(string secretPath, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Retrieving secret from Securitas vault: {Path}", secretPath);

            var requestUrl = $"{_baseUrl}/v1/secret/data/{secretPath}";

            var request = new HttpRequestMessage(HttpMethod.Get, requestUrl);

            // Add bearer token if configured
            if (_useBearerToken && !string.IsNullOrWhiteSpace(_authToken))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _authToken);
            }

            // Note: Client certificate is automatically attached by HttpClientHandler
            // configured in DI registration

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
        catch (HttpRequestException ex) when (ex.Message.Contains("SSL") || ex.Message.Contains("certificate"))
        {
            _logger.LogError(ex,
                "Certificate authentication failed for Securitas vault. " +
                "Verify the client certificate is valid and trusted by the vault server. Path: {Path}",
                secretPath);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve secret from Securitas vault: {Path}", secretPath);
            throw;
        }
    }
}
