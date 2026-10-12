using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Snail.Toolkit.Async.Pipeline;

/// <summary>
/// Runs a projection over a stream with a bounded number of concurrent calls.
/// </summary>
/// <remarks>
/// <para>
/// The bound is on results held in memory, not merely on calls in flight. Results travel through a
/// channel of the requested capacity, so a slow consumer stops the source from being read instead of
/// letting completed results pile up. A semaphore alone bounds only the calls, which is why the
/// previous implementation buffered an entire hundred-thousand-element source while honouring a
/// degree of four.
/// </para>
/// <para>
/// Enumeration owns the work it starts. Leaving the loop — by break, by exception or by cancellation —
/// cancels the pending calls, disposes the source and awaits the pump before returning, so no work
/// outlives the consumer.
/// </para>
/// <para>
/// The first failure wins: it completes the channel, which surfaces it on the consumer's next
/// <c>MoveNextAsync</c>. Nothing is dropped silently.
/// </para>
/// </remarks>
internal static class Concurrent
{
    /// <summary>Projects each element concurrently and yields the results in source order.</summary>
    internal static async IAsyncEnumerable<TResult> OrderedAsync<T, TResult>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        int degree,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var started = Channel.CreateBounded<Task<TResult>>(degree);

        using var work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = StartAsync(source, selector, started.Writer, work.Token);

        try
        {
            await foreach (var pending in started.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return await pending.ConfigureAwait(false);
        }
        finally
        {
            await work.CancelAsync().ConfigureAwait(false);

            while (started.Reader.TryRead(out var abandoned))
                await ObserveAsync(abandoned).ConfigureAwait(false);

            await ObserveAsync(pump).ConfigureAwait(false);
        }
    }

    /// <summary>Projects each element concurrently and yields each result as soon as it is ready.</summary>
    internal static IAsyncEnumerable<TResult> Unordered<T, TResult>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        int degree,
        CancellationToken cancellationToken = default) =>
        FanOutAsync<T, TResult>(
            source,
            async (item, results, token) =>
                await results.WriteAsync(await selector(item, token).ConfigureAwait(false), token)
                    .ConfigureAwait(false),
            degree,
            cancellationToken);

    /// <summary>Expands each element into a sub-sequence concurrently and interleaves the results.</summary>
    internal static IAsyncEnumerable<TResult> Flattened<T, TResult>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, IAsyncEnumerable<TResult>> selector,
        int degree,
        CancellationToken cancellationToken = default) =>
        FanOutAsync<T, TResult>(
            source,
            async (item, results, token) =>
            {
                await foreach (var inner in selector(item, token).WithCancellation(token).ConfigureAwait(false))
                    await results.WriteAsync(inner, token).ConfigureAwait(false);
            },
            degree,
            cancellationToken);

    /// <summary>Tests each element concurrently and yields the matches in source order.</summary>
    internal static async IAsyncEnumerable<T> FilteredAsync<T>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate,
        int degree,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var judged = OrderedAsync(
            source,
            async (item, token) => (Item: item, IsMatch: await predicate(item, token).ConfigureAwait(false)),
            degree,
            cancellationToken);

        await foreach (var (item, isMatch) in judged.ConfigureAwait(false))
        {
            if (isMatch)
                yield return item;
        }
    }

    private static async IAsyncEnumerable<TResult> FanOutAsync<T, TResult>(
        IAsyncEnumerable<T> source,
        Func<T, ChannelWriter<TResult>, CancellationToken, ValueTask> body,
        int degree,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var pending = Channel.CreateBounded<T>(1);
        var results = Channel.CreateBounded<TResult>(degree);

        using var work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var run = RunAsync();

        try
        {
            await foreach (var result in results.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return result;
        }
        finally
        {
            await work.CancelAsync().ConfigureAwait(false);
            await ObserveAsync(run).ConfigureAwait(false);
        }

        yield break;

        async Task RunAsync()
        {
            var running = new Task[degree + 1];
            for (int i = 0; i < degree; i++)
                running[i] = ConsumeAsync(pending.Reader, results.Writer, body, work);

            running[degree] = FeedAsync(source, pending.Writer, work);

            var all = Task.WhenAll(running);

            try
            {
                await all.ConfigureAwait(false);

                results.Writer.TryComplete();
            }
            catch (Exception)
            {
                results.Writer.TryComplete(Failure(all));
            }
        }
    }

    private static async Task StartAsync<T, TResult>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        ChannelWriter<Task<TResult>> started,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
                await started.WriteAsync(InvokeAsync(item, selector, cancellationToken), cancellationToken)
                    .ConfigureAwait(false);

            started.TryComplete();
        }
        catch (Exception failure)
        {
            started.TryComplete(failure);
        }
    }

    private static async Task<TResult> InvokeAsync<T, TResult>(
        T item,
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        CancellationToken cancellationToken) =>
        await selector(item, cancellationToken).ConfigureAwait(false);

    private static async Task FeedAsync<T>(
        IAsyncEnumerable<T> source,
        ChannelWriter<T> pending,
        CancellationTokenSource work)
    {
        try
        {
            await foreach (var item in source.WithCancellation(work.Token).ConfigureAwait(false))
                await pending.WriteAsync(item, work.Token).ConfigureAwait(false);

            pending.TryComplete();
        }
        catch (Exception failure)
        {
            await work.CancelAsync().ConfigureAwait(false);

            pending.TryComplete(failure);
            throw;
        }
    }

    private static async Task ConsumeAsync<T, TResult>(
        ChannelReader<T> pending,
        ChannelWriter<TResult> results,
        Func<T, ChannelWriter<TResult>, CancellationToken, ValueTask> body,
        CancellationTokenSource work)
    {
        try
        {
            await foreach (var item in pending.ReadAllAsync(work.Token).ConfigureAwait(false))
                await body(item, results, work.Token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            await work.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Picks the failure worth reporting out of a group that failed together.</summary>
    /// <remarks>
    /// When one member fails, the others are cancelled deliberately, so their cancellations are
    /// consequences rather than causes. Reporting one of those would name the wrong culprit and hide
    /// the exception the caller needs to see.
    /// </remarks>
    internal static Exception Failure(Task completed)
    {
        var causes = completed.Exception?.InnerExceptions
            .Where(failure => failure is not OperationCanceledException)
            .ToArray() ?? [];

        return causes switch
        {
            [] => completed.Exception as Exception ?? new OperationCanceledException(),
            [var only] => only,
            _ => new AggregateException(causes)
        };
    }

    /// <summary>
    /// Awaits a task that is being abandoned, keeping its failure from replacing the one already travelling.
    /// </summary>
    /// <remarks>
    /// Cleanup runs inside a <c>finally</c>, where a throw would mask the exception the consumer is
    /// already receiving. After cancellation these failures are the cancellation itself, so observing
    /// and discarding them is the whole intent — the real failure has already completed the channel.
    /// </remarks>
    internal static async Task ObserveAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
    }
}
