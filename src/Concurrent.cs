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
    /// <remarks>
    /// The selector's own <see cref="ValueTask{TResult}"/> travels through the channel and is awaited once, by
    /// the consumer or by the cleanup. Wrapping each in a <see cref="Task{TResult}"/> cost a task per element
    /// even when the selector answered at once, and a second state machine when it did not.
    /// </remarks>
    internal static async IAsyncEnumerable<TResult> OrderedAsync<T, TResult>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        int degree,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var started = Channel.CreateBounded<Started<T, TResult>>(degree);

        using var work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = StartAsync(source, selector, started.Writer, work.Token);

        try
        {
            await foreach (var pending in started.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return await pending.Answer.ConfigureAwait(false);
        }
        finally
        {
            await AbandonAsync(started.Reader, pump, work).ConfigureAwait(false);
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
            (pending, results, work) => ProjectAsync(pending, results, selector, work),
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
            (pending, results, work) => ExpandAsync(pending, results, selector, work),
            degree,
            cancellationToken);

    /// <summary>Tests each element concurrently and yields the matches in source order.</summary>
    /// <remarks>
    /// The element travels beside the predicate's own <see cref="ValueTask{TResult}"/>, as a projection's answer
    /// does: pairing them in an async method cost a state machine per element whenever the predicate awaited.
    /// </remarks>
    internal static async IAsyncEnumerable<T> FilteredAsync<T>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate,
        int degree,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var started = Channel.CreateBounded<Started<T, bool>>(degree);

        using var work = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = StartAsync(source, predicate, started.Writer, work.Token);

        try
        {
            await foreach (var pending in started.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await pending.Answer.ConfigureAwait(false))
                    yield return pending.Item;
            }
        }
        finally
        {
            await AbandonAsync(started.Reader, pump, work).ConfigureAwait(false);
        }
    }

    private static async IAsyncEnumerable<TResult> FanOutAsync<T, TResult>(
        IAsyncEnumerable<T> source,
        Func<ChannelReader<T>, ChannelWriter<TResult>, CancellationTokenSource, Task> consume,
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
                running[i] = consume(pending.Reader, results.Writer, work);

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

    /// <summary>Reads the source and starts a call on each element, as fast as the channel takes them.</summary>
    /// <remarks>
    /// A write handed a token that can be cancelled allocates its wait every time the channel is full; a write
    /// without one reuses the channel's single waiting writer, and the pump is the channel's only writer. One
    /// registration for the whole enumeration stops it instead: cancellation completes the channel, which fails
    /// a blocked write at once, as a cancelled token did.
    /// </remarks>
    private static async Task StartAsync<T, TResult>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        ChannelWriter<Started<T, TResult>> started,
        CancellationToken cancellationToken)
    {
        using var stop = cancellationToken.UnsafeRegister(
            static (writer, token) =>
                ((ChannelWriter<Started<T, TResult>>)writer!).TryComplete(new OperationCanceledException(token)),
            started);

        try
        {
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var next = new Started<T, TResult>(item, Invoke(item, selector, cancellationToken));
                cancellationToken.ThrowIfCancellationRequested();

                await started.WriteAsync(next).ConfigureAwait(false);
            }

            started.TryComplete();
        }
        catch (Exception failure)
        {
            started.TryComplete(failure);
        }
    }

    /// <summary>
    /// Stops the calls an ordered enumeration started and awaits each of them and the pump, once the consumer has
    /// left.
    /// </summary>
    private static async Task AbandonAsync<T, TResult>(
        ChannelReader<Started<T, TResult>> started,
        Task pump,
        CancellationTokenSource work)
    {
        await work.CancelAsync().ConfigureAwait(false);

        while (started.TryRead(out var abandoned))
            await ObserveAsync(abandoned.Answer).ConfigureAwait(false);

        await ObserveAsync(pump).ConfigureAwait(false);
    }

    /// <summary>Starts the selector on one element.</summary>
    /// <remarks>
    /// A selector that throws instead of returning a faulted task fails in its place in the sequence, as it
    /// did when an async method wrapped the call: the failure surfaces when the consumer reaches it.
    /// </remarks>
    private static ValueTask<TResult> Invoke<T, TResult>(
        T item,
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        CancellationToken cancellationToken)
    {
        try
        {
            return selector(item, cancellationToken);
        }
        catch (Exception failure)
        {
            return ValueTask.FromException<TResult>(failure);
        }
    }

    /// <summary>Reads the source into the channel the workers take their elements from.</summary>
    /// <remarks>
    /// The feeder is that channel's only writer and the workers wait on it without a token, as
    /// <see cref="StartAsync"/> writes: one registration for the whole enumeration completes the channel on
    /// cancellation, which wakes them all at once, instead of a wait allocated per element and per worker.
    /// </remarks>
    private static async Task FeedAsync<T>(
        IAsyncEnumerable<T> source,
        ChannelWriter<T> pending,
        CancellationTokenSource work)
    {
        using var stop = work.Token.UnsafeRegister(
            static (writer, token) => ((ChannelWriter<T>)writer!).TryComplete(new OperationCanceledException(token)),
            pending);

        try
        {
            await foreach (var item in source.WithCancellation(work.Token).ConfigureAwait(false))
            {
                work.Token.ThrowIfCancellationRequested();

                await pending.WriteAsync(item).ConfigureAwait(false);
            }

            pending.TryComplete();
        }
        catch (Exception failure)
        {
            await work.CancelAsync().ConfigureAwait(false);

            pending.TryComplete(failure);
            throw;
        }
    }

    /// <summary>One of the workers of <see cref="Unordered{T, TResult}"/>: projects elements until none are left.</summary>
    /// <remarks>
    /// The call and the write live in the worker's own loop, so a worker allocates its state machine once for the
    /// whole enumeration; a call per element through an async delegate allocated one each time it awaited.
    /// </remarks>
    private static async Task ProjectAsync<T, TResult>(
        ChannelReader<T> pending,
        ChannelWriter<TResult> results,
        Func<T, CancellationToken, ValueTask<TResult>> selector,
        CancellationTokenSource work)
    {
        try
        {
            await foreach (var item in TakeAsync(pending, work.Token).ConfigureAwait(false))
            {
                var result = await selector(item, work.Token).ConfigureAwait(false);

                await results.WriteAsync(result, work.Token).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            await work.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>One of the workers of <see cref="Flattened{T, TResult}"/>: expands elements until none are left.</summary>
    private static async Task ExpandAsync<T, TResult>(
        ChannelReader<T> pending,
        ChannelWriter<TResult> results,
        Func<T, CancellationToken, IAsyncEnumerable<TResult>> selector,
        CancellationTokenSource work)
    {
        try
        {
            await foreach (var item in TakeAsync(pending, work.Token).ConfigureAwait(false))
            {
                var token = work.Token;

                await foreach (var inner in selector(item, token).WithCancellation(token).ConfigureAwait(false))
                    await results.WriteAsync(inner, token).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            await work.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Reads the channel to its end, as <c>ReadAllAsync</c> does, waiting on it without a token.</summary>
    /// <remarks>
    /// The token is checked before every wait, as a wait handed it would check it; the feeder's registration
    /// completes the channel on cancellation, which ends a wait already begun.
    /// </remarks>
    private static async IAsyncEnumerable<T> TakeAsync<T>(
        ChannelReader<T> pending,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!await pending.WaitToReadAsync().ConfigureAwait(false))
                yield break;

            while (pending.TryRead(out var item))
                yield return item;
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

    /// <summary>Awaits a result that is being abandoned, as <see cref="ObserveAsync(Task)"/> awaits a task.</summary>
    private static async ValueTask ObserveAsync<TResult>(ValueTask<TResult> pending)
    {
        try
        {
            await pending.ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
    }

    /// <summary>An element and the call started on it, travelling together to the consumer.</summary>
    private readonly record struct Started<T, TResult>(T Item, ValueTask<TResult> Answer);
}
