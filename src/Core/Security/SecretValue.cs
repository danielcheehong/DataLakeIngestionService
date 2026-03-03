namespace DataLakeIngestionService.Core.Security;

/// <summary>
/// Holds a sensitive string value (e.g. a vault secret password) in a <c>char[]</c> buffer
/// that can be explicitly zeroed when no longer needed, reducing exposure on the managed heap
/// compared to an immutable <c>string</c> that the GC cannot zero on demand.
///
/// Usage:
///   1. Obtain a <see cref="SecretValue"/> from <c>IVaultService.GetSecretAsync</c> or
///      <c>IConnectionStringBuilder.BuildConnectionStringAsync</c>.
///   2. Call <see cref="Expose"/> only at the point of consumption (e.g. inside a connection
///      constructor) and do NOT store the returned <c>string</c> in a long-lived variable.
///   3. Dispose this instance as soon as it is no longer needed (prefer <c>await using</c>
///      or a <c>using</c> block) so the internal buffer is zeroed immediately.
/// </summary>
public sealed class SecretValue : IDisposable
{
    private char[] _buffer;
    private bool _disposed;

    /// <summary>
    /// Initialises a new <see cref="SecretValue"/> by copying the characters of
    /// <paramref name="rawValue"/> into a new <c>char[]</c> buffer so the buffer
    /// can be independently zeroed via <see cref="Dispose"/>.
    /// </summary>
    public SecretValue(string rawValue)
    {
        ArgumentNullException.ThrowIfNull(rawValue);
        _buffer = rawValue.ToCharArray();
    }

    /// <summary>
    /// Internal constructor that takes direct ownership of an already-allocated
    /// <c>char[]</c> buffer (no copy). Used by <c>ConnectionStringBuilder</c> to
    /// avoid creating an extra intermediate <c>string</c>.
    /// </summary>
    internal SecretValue(char[] buffer)
    {
        _buffer = buffer;
    }

    /// <summary>
    /// Returns <c>true</c> when the buffer is empty (zero length or already disposed).
    /// </summary>
    public bool IsEmpty => _buffer.Length == 0;

    /// <summary>
    /// Materialises a new <c>string</c> from the internal buffer.
    /// Call this only at the immediate point of consumption and do not store the returned
    /// value in a variable that outlives the consuming statement.
    /// </summary>
    /// <exception cref="ObjectDisposedException">Thrown if this instance has been disposed.</exception>
    public string Expose()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new string(_buffer);
    }

    /// <summary>
    /// Returns a copy of the internal buffer as a <c>char[]</c>.
    /// The caller is responsible for calling <see cref="Array.Clear"/> on the returned
    /// array once finished with it.
    /// </summary>
    /// <exception cref="ObjectDisposedException">Thrown if this instance has been disposed.</exception>
    internal char[] CopyBuffer()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return (char[])_buffer.Clone();
    }

    /// <summary>
    /// Zeroes every character in the internal buffer and marks this instance as disposed.
    /// Subsequent calls to <see cref="Expose"/> or <see cref="CopyBuffer"/> will throw
    /// <see cref="ObjectDisposedException"/>.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        Array.Clear(_buffer, 0, _buffer.Length);
        _buffer = Array.Empty<char>();
        _disposed = true;
    }
}
