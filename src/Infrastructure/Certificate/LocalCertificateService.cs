using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using DataLakeIngestionService.Core.Interfaces.Certificate;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DataLakeIngestionService.Infrastructure.Certificate;

/// <summary>
/// Certificate service that retrieves certificates from the local machine's certificate store.
/// </summary>
public class LocalCertificateService : ICertificateService
{
    private readonly ILogger<LocalCertificateService> _logger;
    private readonly CertificateServiceOptions _options;
    private readonly ConcurrentDictionary<string, CachedCertificate> _cache = new();

    public string ProviderName => "LocalCertificateStore";

    public LocalCertificateService(
        ILogger<LocalCertificateService> logger,
        IOptions<CertificateServiceOptions> options)
    {
        _logger = logger;
        _options = options.Value;
    }

    public X509Certificate2? FindByThumbprint(
        string thumbprint,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(thumbprint);

        // Normalize thumbprint (remove spaces, convert to uppercase)
        var normalizedThumbprint = NormalizeThumbprint(thumbprint);
        var cacheKey = $"thumbprint:{normalizedThumbprint}:{storeName}:{storeLocation}";

        // Check cache first
        if (_options.EnableCaching && TryGetFromCache(cacheKey, out var cachedCert))
        {
            _logger.LogDebug("Certificate found in cache: {Thumbprint}", normalizedThumbprint);
            return cachedCert;
        }

        _logger.LogInformation(
            "Searching for certificate by thumbprint: {Thumbprint} in {StoreName}/{StoreLocation}",
            normalizedThumbprint, storeName, storeLocation);

        try
        {
            using var store = new X509Store(storeName, storeLocation);
            store.Open(OpenFlags.ReadOnly);

            var certificates = store.Certificates.Find(
                X509FindType.FindByThumbprint,
                normalizedThumbprint,
                _options.ValidOnly);

            if (certificates.Count == 0)
            {
                _logger.LogWarning(
                    "Certificate not found by thumbprint: {Thumbprint} in {StoreName}/{StoreLocation}",
                    normalizedThumbprint, storeName, storeLocation);
                return null;
            }

            var cert = certificates[0];
            _logger.LogInformation(
                "Certificate found: Subject={Subject}, Expires={NotAfter}",
                cert.Subject, cert.NotAfter);

            // Cache the result
            if (_options.EnableCaching)
            {
                AddToCache(cacheKey, cert);
            }

            return cert;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error searching for certificate by thumbprint: {Thumbprint}",
                normalizedThumbprint);
            throw;
        }
    }

    public X509Certificate2? FindBySubjectName(
        string subjectName,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default)
    {
        var certificates = FindAllBySubjectName(subjectName, storeName, storeLocation, cancellationToken);

        // Return the most recent valid certificate (by NotAfter date)
        return certificates
            .OrderByDescending(c => c.NotAfter)
            .FirstOrDefault();
    }

    public IEnumerable<X509Certificate2> FindAllBySubjectName(
        string subjectName,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subjectName);

        _logger.LogInformation(
            "Searching for certificates by subject name: {SubjectName} in {StoreName}/{StoreLocation}",
            subjectName, storeName, storeLocation);

        try
        {
            using var store = new X509Store(storeName, storeLocation);
            store.Open(OpenFlags.ReadOnly);

            var certificates = store.Certificates.Find(
                X509FindType.FindBySubjectName,
                subjectName,
                _options.ValidOnly);

            _logger.LogInformation(
                "Found {Count} certificate(s) matching subject name: {SubjectName}",
                certificates.Count, subjectName);

            // Return copies since store will be disposed
            var result = new List<X509Certificate2>();
            foreach (var cert in certificates)
            {
                result.Add(cert);
                _logger.LogDebug(
                    "  - Subject={Subject}, Thumbprint={Thumbprint}, Expires={NotAfter}",
                    cert.Subject, cert.Thumbprint, cert.NotAfter);
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error searching for certificates by subject name: {SubjectName}",
                subjectName);
            throw;
        }
    }

    public X509Certificate2 GetRequiredByThumbprint(
        string thumbprint,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default)
    {
        var cert = FindByThumbprint(thumbprint, storeName, storeLocation, cancellationToken);

        if (cert == null)
        {
            throw new InvalidOperationException(
                $"Required certificate not found. Thumbprint: {NormalizeThumbprint(thumbprint)}, " +
                $"Store: {storeName}/{storeLocation}");
        }

        return cert;
    }

    public X509Certificate2 GetRequiredBySubjectName(
        string subjectName,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default)
    {
        var cert = FindBySubjectName(subjectName, storeName, storeLocation, cancellationToken);

        if (cert == null)
        {
            throw new InvalidOperationException(
                $"Required certificate not found. SubjectName: {subjectName}, " +
                $"Store: {storeName}/{storeLocation}");
        }

        return cert;
    }

    /// <summary>
    /// Normalizes thumbprint by removing spaces and converting to uppercase.
    /// </summary>
    private static string NormalizeThumbprint(string thumbprint)
    {
        return thumbprint
            .Replace(" ", string.Empty)
            .Replace("-", string.Empty)
            .ToUpperInvariant();
    }

    private bool TryGetFromCache(string key, out X509Certificate2? certificate)
    {
        if (_cache.TryGetValue(key, out var cached) &&
            cached.ExpiresAt > DateTime.UtcNow)
        {
            certificate = cached.Certificate;
            return true;
        }

        certificate = null;
        return false;
    }

    private void AddToCache(string key, X509Certificate2 certificate)
    {
        var cached = new CachedCertificate
        {
            Certificate = certificate,
            ExpiresAt = DateTime.UtcNow.AddMinutes(_options.CacheDurationMinutes)
        };

        _cache.AddOrUpdate(key, cached, (_, _) => cached);
    }

    private class CachedCertificate
    {
        public required X509Certificate2 Certificate { get; init; }
        public required DateTime ExpiresAt { get; init; }
    }
}
