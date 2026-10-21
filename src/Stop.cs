namespace Snail.Toolkit.Async.Pipeline;

/// <summary>
/// The stop of one enumeration's background work: the token that work observes, and the failure that ordered the
/// stop, when one did.
/// </summary>
/// <remarks>
/// Cancellation is how the members of a fan-out tell each other to stop, so what a member throws after the stop was
/// ordered is a consequence, and the failure recorded here before it is the cause. Telling the two apart by the type
/// of the exception instead mistook a selector's own timeout, which <c>HttpClient</c> reports as a
/// <see cref="TaskCanceledException"/>, for a consequence, and reported a bare cancellation in its place.
/// </remarks>
internal sealed class Stop(CancellationToken cancellationToken) : IDisposable
{
    private readonly CancellationTokenSource _work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    private Exception? _cause;

    /// <summary>The token the work observes: cancelled by the consumer, by the cleanup or by the first failure.</summary>
    public CancellationToken Token => _work.Token;

    /// <summary>The failure that ordered the stop, or null when the consumer or the cleanup did.</summary>
    public Exception? Cause => Volatile.Read(ref _cause);

    /// <summary>Orders the stop because of <paramref name="failure"/>, which is the cause unless a stop was already ordered.</summary>
    public Task FailAsync(Exception failure)
    {
        if (!_work.IsCancellationRequested)
            Interlocked.CompareExchange(ref _cause, failure, null);

        return _work.CancelAsync();
    }

    /// <summary>Orders the stop without a cause: the consumer has left, or asked for it.</summary>
    public Task CancelAsync() => _work.CancelAsync();

    public void Dispose() => _work.Dispose();
}
