using System.Security.Cryptography.X509Certificates;

namespace DataLakeIngestionService.Infrastructure.Vault;

/// <summary>
/// Configuration options for vault certificate authentication.
/// </summary>
public class VaultCertificateOptions
{
    /// <summary>
    /// Enable certificate-based authentication (mTLS).
    /// </summary>
    public bool UseCertificateAuth { get; set; } = false;

    /// <summary>
    /// Certificate thumbprint to search for. Takes precedence over SubjectName.
    /// </summary>
    public string? CertificateThumbprint { get; set; }

    /// <summary>
    /// Certificate subject name (CN) to search for. Used if Thumbprint is not set.
    /// </summary>
    public string? CertificateSubjectName { get; set; }

    /// <summary>
    /// Certificate store name. Defaults to My (Personal).
    /// </summary>
    public StoreName CertificateStoreName { get; set; } = StoreName.My;

    /// <summary>
    /// Certificate store location. Defaults to LocalMachine.
    /// </summary>
    public StoreLocation CertificateStoreLocation { get; set; } = StoreLocation.LocalMachine;

    /// <summary>
    /// When true, also sends the bearer token alongside certificate auth.
    /// Some vaults require both mTLS + token authentication.
    /// </summary>
    public bool UseBearerTokenWithCertificate { get; set; } = true;
}
