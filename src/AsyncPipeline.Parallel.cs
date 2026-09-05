namespace Snail.Toolkit.Async.Pipeline;

public sealed partial class AsyncPipeline<T>
{
    /// <summary>Projects each element concurrently, keeping the source order.</summary>
    /// <remarks>
    /// <paramref name="maxDegreeOfParallelism"/> bounds both the concurrent calls and the results held
    /// in memory, so a slow consumer stops the source from being read. Ordering costs latency: a slow
    /// element delays the ones behind it. Use <see cref="SelectUnordered{TResult}"/> when order is free.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxDegreeOfParallelism"/> is below one.</exception>
    public AsyncPipeline<TResult> Select<TResult>(
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        int maxDegreeOfParallelism)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDegreeOfParallelism, 1);

        return new AsyncPipeline<TResult>(Concurrent.Ordered(source, selector, maxDegreeOfParallelism));
    }

    /// <summary>Projects each element concurrently, yielding each result as soon as it is ready.</summary>
    /// <remarks>
    /// Results arrive in completion order, which is not the source order and is not stable between runs.
    /// A failing element faults the whole sequence rather than disappearing from it.
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
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxDegreeOfParallelism"/> is below one.</exception>
    public AsyncPipeline<T> Where(
        Func<T, CancellationToken, ValueTask<bool>> predicate,
        int maxDegreeOfParallelism)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDegreeOfParallelism, 1);

        return new AsyncPipeline<T>(Concurrent.Filtered(source, predicate, maxDegreeOfParallelism));
    }

    /// <summary>Expands each element into a sub-sequence concurrently and interleaves the results.</summary>
    /// <remarks>
    /// Sub-sequences are read side by side, so their elements interleave. Elements of one sub-sequence
    /// keep their relative order; elements of different sub-sequences do not.
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
