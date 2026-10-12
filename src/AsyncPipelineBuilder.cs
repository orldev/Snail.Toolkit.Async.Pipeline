using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Snail.Toolkit.Async.Pipeline;

/// <summary>
/// Entry points that turn a source into an <see cref="AsyncPipeline{T}"/>.
/// </summary>
public static class AsyncPipelineBuilder
{
    /// <summary>Wraps an asynchronous sequence.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    public static AsyncPipeline<T> From<T>(IAsyncEnumerable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new AsyncPipeline<T>(source);
    }

    /// <summary>Wraps a synchronous collection.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    public static AsyncPipeline<T> From<T>(IEnumerable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new AsyncPipeline<T>(source.ToAsyncEnumerable());
    }

    /// <summary>Wraps a collection that is still being fetched.</summary>
    /// <remarks>
    /// The task is awaited once per enumeration and its result replayed, so the fetch happens when the
    /// pipeline is first read rather than when it is built.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    public static AsyncPipeline<T> From<T>(Task<IEnumerable<T>> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new AsyncPipeline<T>(AwaitedAsync(source));
    }

    /// <summary>Wraps a single value that is still being produced.</summary>
    /// <remarks>
    /// Takes a factory rather than a <see cref="ValueTask{TResult}"/> because a pipeline is enumerable
    /// more than once and a <see cref="ValueTask{TResult}"/> may be consumed only once. Holding one
    /// across enumerations throws, or — when its backing token has been recycled — quietly returns
    /// another operation's result.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    public static AsyncPipeline<T> From<T>(Func<ValueTask<T>> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new AsyncPipeline<T>(SingleAsync<T>(_ => source()));
    }

    /// <summary>Wraps a single value that is still being produced, cancelled with the enumeration.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    public static AsyncPipeline<T> From<T>(Func<CancellationToken, ValueTask<T>> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new AsyncPipeline<T>(SingleAsync(source));
    }

    /// <summary>Reads every source at once and yields elements as they arrive.</summary>
    /// <remarks>
    /// Order is not preserved, and it is not stable between runs. The first source to fail faults the
    /// merged sequence and stops the others; if several fail together their exceptions are reported
    /// as one <see cref="AggregateException"/>.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/>, or one of its elements, is null.</exception>
    public static AsyncPipeline<T> Merge<T>(params IAsyncEnumerable<T>[] sources)
    {
        ArgumentNullException.ThrowIfNull(sources);
        foreach (var source in sources)
            ArgumentNullException.ThrowIfNull(source);

        return new AsyncPipeline<T>(MergedAsync(sources));
    }

    /// <summary>Reads the sources one after another, in the order given.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/>, or one of its elements, is null.</exception>
    public static AsyncPipeline<T> Concat<T>(params IAsyncEnumerable<T>[] sources)
    {
        ArgumentNullException.ThrowIfNull(sources);
        foreach (var source in sources)
            ArgumentNullException.ThrowIfNull(source);

        return new AsyncPipeline<T>(ConcatenatedAsync(sources));
    }

    /// <remarks>
    /// The task was started by the caller, so cancelling the enumeration cannot stop it — it only stops
    /// this pipeline waiting on it, which is why the token goes to <c>WaitAsync</c> rather than being
    /// ignored as a task nobody here owns.
    /// </remarks>
    private static async IAsyncEnumerable<T> AwaitedAsync<T>(
        Task<IEnumerable<T>> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var items = await source.WaitAsync(cancellationToken).ConfigureAwait(false);

        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();

            yield return item;
        }
    }

    private static async IAsyncEnumerable<T> SingleAsync<T>(
        Func<CancellationToken, ValueTask<T>> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return await source(cancellationToken).ConfigureAwait(false);
    }

    private static async IAsyncEnumerable<T> MergedAsync<T>(
        IAsyncEnumerable<T>[] sources,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (sources.Length == 0)
            yield break;

        var merged = Channel.CreateBounded<T>(sources.Length);

        using var work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var readers = ReadAllAsync(sources, merged.Writer, work);

        try
        {
            await foreach (var item in merged.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return item;
        }
        finally
        {
            await work.CancelAsync().ConfigureAwait(false);
            await Concurrent.ObserveAsync(readers).ConfigureAwait(false);
        }
    }

    private static async Task ReadAllAsync<T>(
        IAsyncEnumerable<T>[] sources,
        ChannelWriter<T> merged,
        CancellationTokenSource work)
    {
        var readers = new Task[sources.Length];
        for (var i = 0; i < sources.Length; i++)
            readers[i] = ReadAsync(sources[i], merged, work);

        var all = Task.WhenAll(readers);

        try
        {
            await all.ConfigureAwait(false);

            merged.TryComplete();
        }
        catch (Exception)
        {
            merged.TryComplete(Concurrent.Failure(all));
        }
    }

    private static async Task ReadAsync<T>(
        IAsyncEnumerable<T> source,
        ChannelWriter<T> merged,
        CancellationTokenSource work)
    {
        try
        {
            await foreach (var item in source.WithCancellation(work.Token).ConfigureAwait(false))
                await merged.WriteAsync(item, work.Token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            await work.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static async IAsyncEnumerable<T> ConcatenatedAsync<T>(
        IAsyncEnumerable<T>[] sources,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var source in sources)
        {
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
                yield return item;
        }
    }
}
