namespace Snail.Toolkit.Async.Pipeline;

public sealed partial class AsyncPipeline<T>
{
    /// <summary>Projects each element concurrently, keeping the source order.</summary>
    /// <remarks>
    /// <paramref name="maxDegreeOfParallelism"/> bounds the calls in flight exactly, and with them the results held
    /// in memory: a call starts only once a slot is free, and the slot stays taken until the consumer has its result,
    /// so a slow consumer stops the source from being read. Ordering costs latency: a slow element delays the ones
    /// behind it. Use <see cref="SelectUnordered{TResult}"/> when order is free.
    /// <para>
    /// Each <see cref="ValueTask{TResult}"/> the selector returns is awaited exactly once, but not at once: it is
    /// held until the consumer reaches its element. A selector must not return value tasks backed by one reused
    /// source, which was never safe above a degree of one.
    /// </para>
    /// <para>
    /// A selector's own cancellation — a timeout — is reported as it was thrown. When the enumeration's token is
    /// cancelled, the exception the consumer receives carries that token, whatever token the selector threw with.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxDegreeOfParallelism"/> is below one.</exception>
    public AsyncPipeline<TResult> Select<TResult>(
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        int maxDegreeOfParallelism)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDegreeOfParallelism, 1);

        return new AsyncPipeline<TResult>(Concurrent.OrderedAsync(source, selector, maxDegreeOfParallelism));
    }

    /// <summary>Projects each element concurrently, yielding each result as soon as it is ready.</summary>
    /// <remarks>
    /// Results arrive in completion order, which is not the source order and is not stable between runs.
    /// A failing element faults the whole sequence rather than disappearing from it, and a selector's own
    /// cancellation — a timeout — is reported as it was thrown.
    /// <para>
    /// <paramref name="maxDegreeOfParallelism"/> bounds the calls in flight exactly; a worker is started as elements
    /// arrive, so a degree above the source costs nothing. The results held in memory reach twice the degree and
    /// one — the channel, a finished result each worker waits to hand over, and the one being yielded — and the
    /// source is read at most two elements ahead of the workers.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxDegreeOfParallelism"/> is below one.</exception>
    public AsyncPipeline<TResult> SelectUnordered<TResult>(
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        int maxDegreeOfParallelism)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDegreeOfParallelism, 1);

        return new AsyncPipeline<TResult>(Concurrent.Unordered(source, selector, maxDegreeOfParallelism));
    }

    /// <summary>Tests each element concurrently, keeping the source order.</summary>
    /// <remarks>
    /// <paramref name="maxDegreeOfParallelism"/> bounds the calls in flight exactly, as it does for
    /// <see cref="Select{TResult}(Func{T, CancellationToken, ValueTask{TResult}}, int)"/>, and the elements held
    /// with their pending answers.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxDegreeOfParallelism"/> is below one.</exception>
    public AsyncPipeline<T> Where(
        Func<T, CancellationToken, ValueTask<bool>> predicate,
        int maxDegreeOfParallelism)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDegreeOfParallelism, 1);

        return new AsyncPipeline<T>(Concurrent.FilteredAsync(source, predicate, maxDegreeOfParallelism));
    }

    /// <summary>Expands each element into a sub-sequence concurrently and interleaves the results.</summary>
    /// <remarks>
    /// Sub-sequences are read side by side, so their elements interleave. Elements of one sub-sequence
    /// keep their relative order; elements of different sub-sequences do not. The bounds are those of
    /// <see cref="SelectUnordered{TResult}"/>: at most <paramref name="maxDegreeOfParallelism"/> sub-sequences open
    /// at once, up to twice the degree and one of their elements held.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxDegreeOfParallelism"/> is below one.</exception>
    public AsyncPipeline<TResult> SelectMany<TResult>(
        Func<T, CancellationToken, IAsyncEnumerable<TResult>> selector,
        int maxDegreeOfParallelism)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDegreeOfParallelism, 1);

        return new AsyncPipeline<TResult>(Concurrent.Flattened(source, selector, maxDegreeOfParallelism));
    }
}
