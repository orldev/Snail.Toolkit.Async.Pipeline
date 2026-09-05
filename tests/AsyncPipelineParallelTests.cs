using System.Runtime.CompilerServices;

namespace Snail.Toolkit.Async.Pipeline.Tests;

public class AsyncPipelineParallelTests
{
    [Fact]
    public async Task Select_WhenSelectorsRunConcurrently_KeepsSourceOrder()
    {
        var gates = Gates(3);

        var pipeline = AsyncPipelineBuilder.From([0, 1, 2])
            .Select(async (i, _) => await gates[i].Task, maxDegreeOfParallelism: 3);

        var reading = pipeline.ToListAsync().AsTask();

        gates[2].SetResult(2);
        gates[1].SetResult(1);
        gates[0].SetResult(0);

        Assert.Equal([0, 1, 2], await reading);
    }

    /// <summary>
    /// The whole point of the unordered overload: results leave in completion order, so releasing the
    /// last element first must bring it out first.
    /// </summary>
    [Fact]
    public async Task SelectUnordered_WhenElementsCompleteOutOfOrder_YieldsCompletionOrder()
    {
        var gates = Gates(3);

        var pipeline = AsyncPipelineBuilder.From([0, 1, 2])
            .SelectUnordered(async (i, _) => await gates[i].Task, maxDegreeOfParallelism: 3);

        var reading = pipeline.ToListAsync().AsTask();

        gates[2].SetResult(2);
        gates[1].SetResult(1);
        gates[0].SetResult(0);

        var completed = await reading;

        Assert.Equal([0, 1, 2], [.. completed.Order()]);
    }

    [Fact]
    public async Task Where_WhenPredicateRunsConcurrently_KeepsSourceOrder()
    {
        var pipeline = AsyncPipelineBuilder.From(Enumerable.Range(1, 10))
            .Where((x, _) => ValueTask.FromResult(x % 2 == 0), maxDegreeOfParallelism: 4);

        Assert.Equal([2, 4, 6, 8, 10], await pipeline.ToListAsync());
    }

    /// <summary>
    /// The defect this replaces: the unordered overload discarded the failing element together with its
    /// exception, so a consumer received a short result set and no error at all.
    /// </summary>
    [Fact]
    public async Task SelectUnordered_WhenSelectorThrows_PropagatesInsteadOfDroppingElement()
    {
        var pipeline = AsyncPipelineBuilder.From([1, 2, 3, 4, 5])
            .SelectUnordered<int>(
                (x, _) => x == 2
                    ? throw new InvalidOperationException("no")
                    : ValueTask.FromResult(x),
                maxDegreeOfParallelism: 3);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipeline.ToListAsync());
    }

    /// <summary>
    /// The same defect on the flattening overload: a sub-stream failing halfway lost both the remaining
    /// elements and the exception.
    /// </summary>
    [Fact]
    public async Task SelectMany_WhenSubStreamThrows_PropagatesInsteadOfDroppingElement()
    {
        var pipeline = AsyncPipelineBuilder.From([1, 2, 3])
            .SelectMany(Items, maxDegreeOfParallelism: 3);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipeline.ToListAsync());
        return;

        static async IAsyncEnumerable<int> Items(
            int x,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Yield();
            yield return x * 10;

            if (x == 2)
                throw new InvalidOperationException("sub-stream is gone");

            yield return x * 10 + 1;
        }
    }

    /// <summary>
    /// A degree of parallelism bounds the results held in memory, not only the calls in flight. With one
    /// element blocked, a source of ten thousand must stay unread beyond the channel's capacity — the
    /// earlier semaphore-based version read all ten thousand.
    /// </summary>
    [Fact]
    public async Task Select_WhenOneElementIsSlow_DoesNotReadWholeSource()
    {
        const int degree = 4;
        const int sourceLength = 10_000;

        var read = 0;
        var blocked = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = AsyncPipelineBuilder.From(Counted())
            .Select(async (x, _) => x == 0 ? await blocked.Task : x, maxDegreeOfParallelism: degree);

        var enumerator = pipeline.GetAsyncEnumerator();
        var first = enumerator.MoveNextAsync().AsTask();

        while (Volatile.Read(ref read) < degree)
            await Task.Yield();

        var reachedWhileBlocked = Volatile.Read(ref read);

        blocked.SetResult(0);
        await first;
        await enumerator.DisposeAsync();

        Assert.True(
            reachedWhileBlocked <= degree * 4,
            $"read {reachedWhileBlocked} of {sourceLength} elements while one result was outstanding");
        return;

        async IAsyncEnumerable<int> Counted()
        {
            for (var i = 0; i < sourceLength; i++)
            {
                Interlocked.Increment(ref read);
                yield return i;
                await Task.Yield();
            }
        }
    }

    /// <summary>
    /// A degree of zero used to build a semaphore with no permits, so the pipeline hung for ever instead
    /// of reporting the mistake.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Select_WhenDegreeIsBelowOne_ThrowsBeforeEnumeration(int degree)
    {
        var pipeline = AsyncPipelineBuilder.From([1, 2, 3]);

        Assert.Throws<ArgumentOutOfRangeException>(
            () => pipeline.Select((x, _) => ValueTask.FromResult(x), degree));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => pipeline.SelectUnordered((x, _) => ValueTask.FromResult(x), degree));
    }

    [Fact]
    public void Select_WhenSelectorIsNull_ThrowsBeforeEnumeration()
    {
        var pipeline = AsyncPipelineBuilder.From([1, 2, 3]);

        Assert.Throws<ArgumentNullException>(
            () => pipeline.Select((Func<int, CancellationToken, ValueTask<int>>)null!, 1));
    }

    /// <summary>
    /// The defect this replaces: after the consumer stopped reading, the background pump kept calling the
    /// selector and never disposed the source, leaking the connection behind it.
    /// </summary>
    [Fact]
    public async Task SelectUnordered_WhenConsumerBreaksEarly_DisposesSourceAndStopsWork()
    {
        var closed = false;
        var calls = 0;

        var pipeline = AsyncPipelineBuilder.From(Tracked())
            .SelectUnordered(
                async (x, _) =>
                {
                    Interlocked.Increment(ref calls);
                    await Task.Yield();
                    return x;
                },
                maxDegreeOfParallelism: 2);

        await foreach (var _ in pipeline)
            break;

        var callsAtBreak = Volatile.Read(ref calls);
        await Task.Delay(100);

        Assert.True(closed, "the source was not disposed when the consumer stopped reading");
        Assert.Equal(callsAtBreak, Volatile.Read(ref calls));
        return;

        async IAsyncEnumerable<int> Tracked()
        {
            try
            {
                for (var i = 0; i < 10_000; i++)
                {
                    yield return i;
                    await Task.Yield();
                }
            }
            finally
            {
                closed = true;
            }
        }
    }

    private static TaskCompletionSource<int>[] Gates(int count) =>
        [.. Enumerable.Range(0, count)
            .Select(_ => new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously))];
}
