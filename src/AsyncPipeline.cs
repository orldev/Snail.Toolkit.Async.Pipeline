using System.Runtime.CompilerServices;

namespace Snail.Toolkit.Async.Pipeline;

/// <summary>
/// A fluent view over an asynchronous sequence.
/// </summary>
/// <remarks>
/// <para>
/// The pipeline is a description of work, not a running one: nothing is read until it is enumerated,
/// and each enumeration starts the whole chain again. Whether a second enumeration is legal is the
/// source's business — a pipeline over a network stream or a database cursor is single-use, and
/// enumerating it twice fails or comes back empty.
/// </para>
/// <para>
/// The cancellation token is supplied at enumeration, through <see cref="GetAsyncEnumerator"/> or
/// <see cref="WithCancellation"/>, and reaches every operator in the chain. Operators do not capture
/// a token when the chain is built.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of the elements.</typeparam>
/// <param name="source">The sequence being wrapped.</param>
public sealed partial class AsyncPipeline<T>(IAsyncEnumerable<T> source) : IAsyncEnumerable<T>
{
    /// <summary>Returns the sequence underneath, for code that wants the bare interface.</summary>
    public IAsyncEnumerable<T> AsAsyncEnumerable() => source;

    /// <summary>Binds a cancellation token to the enumeration about to start.</summary>
    /// <remarks>
    /// The continuation behaviour stays the caller's to choose: chain <c>ConfigureAwait</c> on the
    /// result rather than having the library decide it for the consuming loop.
    /// </remarks>
    public ConfiguredCancelableAsyncEnumerable<T> WithCancellation(CancellationToken cancellationToken)
        => source.WithCancellation(cancellationToken);

    /// <summary>Starts an enumeration, cancelled through <paramref name="cancellationToken"/>.</summary>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        => source.GetAsyncEnumerator(cancellationToken);
}
