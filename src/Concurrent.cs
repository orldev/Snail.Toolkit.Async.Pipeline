using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Snail.Toolkit.Async.Pipeline;

/// <summary>
/// Runs a projection over a stream with a bounded number of concurrent calls.
/// </summary>
/// <remarks>
/// <para>
/// The bound is on the calls in flight and on the results held in memory, not merely on the calls. Results travel
/// through a channel of the requested capacity, so a slow consumer stops the source from being read instead of
/// letting completed results pile up. A semaphore alone bounds only the calls, which is why the previous
/// implementation buffered an entire hundred-thousand-element source while honouring a degree of four.
/// </para>
/// <para>
/// Enumeration owns the work it starts. Leaving the loop — by break, by exception or by cancellation —
/// cancels the pending calls, disposes the source and awaits the pump before returning, so no work
/// outlives the consumer. The wait is for the source and the calls to answer their token: one that ignores it
/// holds the return until it answers.
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
        await foreach (var answered in AnsweredAsync(source, selector, degree, cancellationToken).ConfigureAwait(false))
            yield return answered.Answer;
    }

    /// <summary>Tests each element concurrently and yields the matches in source order.</summary>
    internal static async IAsyncEnumerable<T> FilteredAsync<T>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate,
        int degree,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var answered in AnsweredAsync(source, predicate, degree, cancellationToken).ConfigureAwait(false))
        {
            if (answered.Answer)
                yield return answered.Item;
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
            (pending, results, stop) => ProjectAsync(pending, results, selector, stop),
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
            (pending, results, stop) => ExpandAsync(pending, results, selector, stop),
            degree,
            cancellationToken);

    /// <summary>Starts a call on each element in source order, at most <paramref name="degree"/> at once, and yields each element with its answer.</summary>
    /// <remarks>
    /// <para>
    /// The element travels beside the selector's own <see cref="ValueTask{TResult}"/>, which is awaited once, by the
    /// consumer or by the cleanup. Wrapping each in a <see cref="Task{TResult}"/> cost a task per element even when
    /// the selector answered at once, and a second state machine when it did not.
    /// </para>
    /// <para>
    /// The slot a call takes in the channel stays taken while its answer is awaited, and the pump starts a call only
    /// once a slot is free, so the degree bounds the calls exactly. Reading the call out before awaiting it, as the
    /// previous version did, freed the slot early, and with the call the pump had started before waiting for a slot
    /// a degree of one ran three calls at once.
    /// </para>
    /// <para>
    /// A selector that honours its token throws with the enumeration's linked token when the consumer cancels; the
    /// consumer is handed its own token instead, which is how a caller tells its cancellation from a timeout inside
    /// the call.
    /// </para>
    /// </remarks>
    private static async IAsyncEnumerable<Answered<T, TResult>> AnsweredAsync<T, TResult>(
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
            while (await started.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (started.Reader.TryPeek(out var pending))
                {
                    TResult answer;

                    try
                    {
                        answer = await pending.Answer.ConfigureAwait(false);
                    }
                    catch (OperationCanceledException stopped)
                        when (cancellationToken.IsCancellationRequested && stopped.CancellationToken != cancellationToken)
                    {
                        throw new OperationCanceledException(stopped.Message, stopped, cancellationToken);
                    }
                    finally
                    {
                        started.Reader.TryRead(out _);
                    }

                    yield return new Answered<T, TResult>(pending.Item, answer);
                }
            }
        }
        finally
        {
            await AbandonAsync(started.Reader, pump, work).ConfigureAwait(false);
        }
    }

    private static async IAsyncEnumerable<TResult> FanOutAsync<T, TResult>(
        IAsyncEnumerable<T> source,
        Func<ChannelReader<T>, ChannelWriter<TResult>, Stop, Task> consume,
        int degree,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var pending = Channel.CreateBounded<T>(1);
        var results = Channel.CreateBounded<TResult>(degree);

        using var stop = new Stop(cancellationToken);
        var run = RunAsync();

        try
        {
            await foreach (var result in results.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return result;
        }
        finally
        {
            await stop.CancelAsync().ConfigureAwait(false);
            await ObserveAsync(run).ConfigureAwait(false);
        }

        yield break;

        async Task RunAsync()
        {
            var running = new Task[degree + 1];
            for (int i = 0; i < degree; i++)
                running[i] = consume(pending.Reader, results.Writer, stop);

            running[degree] = FeedAsync(source, pending.Writer, stop);

            var all = Task.WhenAll(running);

            try
            {
                await all.ConfigureAwait(false);

                results.Writer.TryComplete();
            }
            catch (Exception)
            {
                results.Writer.TryComplete(Failure(all, stop.Cause));
            }
        }
    }

    /// <summary>Reads the source and starts a call on each element, as fast as the channel takes them.</summary>
    /// <remarks>
    /// A write handed a token that can be cancelled allocates its wait every time the channel is full; a wait
    /// without one reuses the channel's single waiting writer, and the pump is the channel's only writer. One
    /// registration for the whole enumeration stops it instead: cancellation completes the channel, which fails a
    /// blocked wait at once, as a cancelled token did. The pump waits for a slot before it starts the call, so a call
    /// is never started that the channel cannot take; the write after a wait fails only when the channel was completed
    /// in between, and the call started for it is awaited before the pump leaves.
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
                if (!await started.WaitToWriteAsync().ConfigureAwait(false))
                    break;

                var next = new Started<T, TResult>(item, Invoke(item, selector, cancellationToken));

                if (started.TryWrite(next))
                    continue;

                await ObserveAsync(next.Answer).ConfigureAwait(false);
                break;
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
        Stop stop)
    {
        using var halt = stop.Token.UnsafeRegister(
            static (writer, token) => ((ChannelWriter<T>)writer!).TryComplete(new OperationCanceledException(token)),
            pending);

        try
        {
            await foreach (var item in source.WithCancellation(stop.Token).ConfigureAwait(false))
            {
                stop.Token.ThrowIfCancellationRequested();

                await pending.WriteAsync(item).ConfigureAwait(false);
            }

            pending.TryComplete();
        }
        catch (Exception failure)
        {
            await stop.FailAsync(failure).ConfigureAwait(false);

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
        Stop stop)
    {
        try
        {
            await foreach (var item in TakeAsync(pending, stop.Token).ConfigureAwait(false))
            {
                var result = await selector(item, stop.Token).ConfigureAwait(false);

                await results.WriteAsync(result, stop.Token).ConfigureAwait(false);
            }
        }
        catch (Exception failure)
        {
            await stop.FailAsync(failure).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>One of the workers of <see cref="Flattened{T, TResult}"/>: expands elements until none are left.</summary>
    private static async Task ExpandAsync<T, TResult>(
        ChannelReader<T> pending,
        ChannelWriter<TResult> results,
        Func<T, CancellationToken, IAsyncEnumerable<TResult>> selector,
        Stop stop)
    {
        try
        {
            await foreach (var item in TakeAsync(pending, stop.Token).ConfigureAwait(false))
            {
                var token = stop.Token;

                await foreach (var inner in selector(item, token).WithCancellation(token).ConfigureAwait(false))
                    await results.WriteAsync(inner, token).ConfigureAwait(false);
            }
        }
        catch (Exception failure)
        {
            await stop.FailAsync(failure).ConfigureAwait(false);
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
    /// The cause is the failure recorded before the stop was ordered, whatever its type: a selector's own timeout is
    /// a cancellation by type and a cause all the same. The others are cancelled deliberately after it, so their
    /// cancellations are consequences; any other exception thrown beside the cause is reported with it.
    /// </remarks>
    internal static Exception Failure(Task completed, Exception? cause)
    {
        var thrown = completed.Exception?.InnerExceptions.AsEnumerable() ?? [];
        var others = thrown
            .Where(failure => failure is not OperationCanceledException && failure != cause)
            .ToArray();
        Exception[] causes = cause is null ? others : [cause, .. others];

        return causes switch
        {
            [] => completed.Exception as Exception ?? new OperationCanceledException(),
            [var only] => only,
            _ => new AggregateException(causes),
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

    /// <summary>An element and what the call on it answered.</summary>
    private readonly record struct Answered<T, TResult>(T Item, TResult Answer);
}
