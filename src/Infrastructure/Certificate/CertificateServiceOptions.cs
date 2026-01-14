using System.Security.Cryptography.X509Certificates;

namespace DataLakeIngestionService.Infrastructure.Certificate;

/// <summary>
/// Configuration options for the certificate service.
/// </summary>
public class CertificateServiceOptions
{
    /// <summary>
    /// Default certificate store name. Defaults to My (Personal).
    /// </summary>
    public StoreName DefaultStoreName { get; set; } = StoreName.My;

    /// <summary>
    /// Default certificate store location. Defaults to LocalMachine.
    /// </summary>
    public StoreLocation DefaultStoreLocation { get; set; } = StoreLocation.LocalMachine;

    /// <summary>
    /// When true, only returns certificates that are currently valid (not expired, valid chain).
    /// Defaults to false to allow finding expired certificates for diagnostics.
    /// </summary>
    public bool ValidOnly { get; set; } = false;

    /// <summary>
    /// Enable in-memory caching of certificates. Defaults to false.
    /// </summary>
    public bool EnableCaching { get; set; } = false;

    /// <summary>
    /// Cache duration in minutes. Defaults to 5 minutes.
    /// </summary>
    public int CacheDurationMinutes { get; set; } = 5;
}
