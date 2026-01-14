using System.Security.Cryptography.X509Certificates;

namespace DataLakeIngestionService.Core.Interfaces.Certificate;

/// <summary>
/// Service for retrieving X.509 certificates from certificate stores.
/// </summary>
public interface ICertificateService
{
    /// <summary>
    /// Gets the certificate provider name.
    /// </summary>
    string ProviderName { get; }

    /// <summary>
    /// Finds a certificate by its thumbprint.
    /// </summary>
    /// <param name="thumbprint">The certificate thumbprint (SHA-1 hash).</param>
    /// <param name="storeName">The certificate store name. Defaults to My (Personal).</param>
    /// <param name="storeLocation">The certificate store location. Defaults to LocalMachine.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The certificate if found; otherwise null.</returns>
    X509Certificate2? FindByThumbprint(
        string thumbprint,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Finds a certificate by its subject name.
    /// Returns the most recent valid certificate if multiple matches exist.
    /// </summary>
    /// <param name="subjectName">The certificate subject name (CN).</param>
    /// <param name="storeName">The certificate store name. Defaults to My (Personal).</param>
    /// <param name="storeLocation">The certificate store location. Defaults to LocalMachine.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The certificate if found; otherwise null.</returns>
    X509Certificate2? FindBySubjectName(
        string subjectName,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Finds all certificates matching a subject name.
    /// </summary>
    /// <param name="subjectName">The certificate subject name (CN).</param>
    /// <param name="storeName">The certificate store name. Defaults to My (Personal).</param>
    /// <param name="storeLocation">The certificate store location. Defaults to LocalMachine.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Collection of matching certificates.</returns>
    IEnumerable<X509Certificate2> FindAllBySubjectName(
        string subjectName,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a certificate by thumbprint or throws if not found.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when certificate is not found.</exception>
    X509Certificate2 GetRequiredByThumbprint(
        string thumbprint,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a certificate by subject name or throws if not found.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when certificate is not found.</exception>
    X509Certificate2 GetRequiredBySubjectName(
        string subjectName,
        StoreName storeName = StoreName.My,
        StoreLocation storeLocation = StoreLocation.LocalMachine,
        CancellationToken cancellationToken = default);
}
