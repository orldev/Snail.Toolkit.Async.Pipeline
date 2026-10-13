namespace Snail.Toolkit.Async.Pipeline.Tests;

/// <summary>
/// How the parallel operators stop: by the caller's token, by a consumer that leaves, by a failure of the source or
/// of a call. Each stop closes the source, starts no more calls, and reports the cause itself.
/// </summary>
public class AsyncPipelineStoppingTests
{
    [Fact]
    public async Task Select_WhenTokenIsCancelledMidStream_ThrowsTheCallersCancellationAndClosesTheSource()
    {
        var source = new Tracked();
        using var cancellation = new CancellationTokenSource();
        var read = 0;

        var stopped = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var pipeline = source.Endless().ToPipeline().Select((x, _) => ValueTask.FromResult(x), 4);

            await foreach (var _ in pipeline.WithCancellation(cancellation.Token))
            {
                if (++read == 10)
                    await cancellation.CancelAsync();
            }
        });

        Assert.Equal(cancellation.Token, stopped.CancellationToken);
        Assert.True(source.IsClosed);
    }

    [Fact]
    public async Task Select_WhenConsumerBreaksEarly_ClosesTheSourceAndStartsNoMoreCalls()
    {
        var source = new Tracked();
        var calls = 0;

        var pipeline = source.Endless().ToPipeline().Select(
            async (x, _) =>
            {
                Interlocked.Increment(ref calls);
                await Task.Yield();
                return x;
            },
            4);

        await foreach (var _ in pipeline)
            break;

        var callsAtBreak = Volatile.Read(ref calls);
        await Task.Delay(50);

        Assert.True(source.IsClosed);
        Assert.Equal(callsAtBreak, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task Select_WhenSourceFails_YieldsWhatCameBeforeThenTheFailureItself()
    {
        var failure = new InvalidOperationException("source is gone");
        List<int> received = [];

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var pipeline = FailingAfter(3, failure).ToPipeline().Select((x, _) => ValueTask.FromResult(x), 4);

            await foreach (var value in pipeline)
                received.Add(value);
        });

        Assert.Same(failure, thrown);
        Assert.Equal([0, 1, 2], received);
    }

    [Fact]
    public async Task Where_WhenPredicateThrows_YieldsTheMatchesBeforeItThenTheFailure()
    {
        List<int> received = [];

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var pipeline = Enumerable.Range(1, 100).ToPipeline().Where(
                (x, _) => x == 7 ? throw new InvalidOperationException("no") : ValueTask.FromResult(x % 2 == 0),
                4);

            await foreach (var value in pipeline)
                received.Add(value);
        });

        Assert.Equal([2, 4, 6], received);
    }

    [Fact]
    public async Task Where_WhenTokenIsCancelledMidStream_ThrowsTheCallersCancellationAndClosesTheSource()
    {
        var source = new Tracked();
        using var cancellation = new CancellationTokenSource();
        var read = 0;

        var stopped = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var pipeline = source.Endless().ToPipeline().Where((x, _) => ValueTask.FromResult(x % 2 == 0), 4);

            await foreach (var _ in pipeline.WithCancellation(cancellation.Token))
            {
                if (++read == 10)
                    await cancellation.CancelAsync();
            }
        });

        Assert.Equal(cancellation.Token, stopped.CancellationToken);
        Assert.True(source.IsClosed);
    }

    [Fact]
    public async Task SelectUnordered_WhenTokenIsCancelledMidStream_ThrowsTheCallersCancellationAndClosesTheSource()
    {
        var source = new Tracked();
        using var cancellation = new CancellationTokenSource();
        var read = 0;

        var stopped = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var pipeline = source.Endless().ToPipeline().SelectUnordered((x, _) => ValueTask.FromResult(x), 4);

            await foreach (var _ in pipeline.WithCancellation(cancellation.Token))
            {
                if (++read == 10)
                    await cancellation.CancelAsync();
            }
        });

        Assert.Equal(cancellation.Token, stopped.CancellationToken);
        Assert.True(source.IsClosed);
    }

    [Fact]
    public async Task SelectUnordered_WhenSourceFails_ReportsTheFailureItself()
    {
        var failure = new InvalidOperationException("source is gone");

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await FailingAfter(50, failure).ToPipeline()
                .SelectUnordered((x, _) => ValueTask.FromResult(x), 4)
                .ToListAsync());

        Assert.Same(failure, thrown);
    }

    [Fact]
    public async Task SelectUnordered_WhenSelectorFailsAfterAwaiting_ReportsThatFailureAlone()
    {
        var source = new Tracked();
        var failure = new InvalidOperationException("no");

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await source.Endless().ToPipeline()
                .SelectUnordered<int>(
                    async (x, _) =>
                    {
                        await Task.Yield();

                        return x == 20 ? throw failure : x;
                    },
                    4)
                .ToListAsync());

        Assert.Same(failure, thrown);
        Assert.True(source.IsClosed);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(8)]
    public async Task SelectUnordered_WhenSourceEnds_YieldsEveryResultOnce(int degree)
    {
        var results = await Enumerable.Range(0, 1_000).ToPipeline()
            .SelectUnordered((x, _) => x % 3 == 0 ? Later(x) : ValueTask.FromResult(x), degree)
            .ToListAsync();

        Assert.Equal(Enumerable.Range(0, 1_000), results.Order());
    }

    [Fact]
    public async Task SelectMany_WhenConsumerBreaksEarly_ClosesTheSourceAndTheSubSequences()
    {
        var source = new Tracked();
        var inner = new Tracked();

        var pipeline = source.Endless().ToPipeline().SelectMany((_, _) => inner.Endless(), 3);

        await foreach (var _ in pipeline)
            break;

        Assert.True(source.IsClosed);
        Assert.True(inner.IsClosed);
        Assert.Equal(0, inner.OpenCount);
    }

    [Fact]
    public async Task SelectMany_WhenSourceEnds_YieldsEveryElementOfEverySubSequence()
    {
        var results = await Enumerable.Range(0, 300).ToPipeline()
            .SelectMany((x, _) => Twice(x), 4)
            .ToListAsync();

        Assert.Equal(Enumerable.Range(0, 300).SelectMany(x => new[] { x, x }), results.Order());
    }

    private static async ValueTask<int> Later(int value)
    {
        await Task.Yield();

        return value;
    }

    private static async IAsyncEnumerable<int> Twice(int value)
    {
        yield return value;

        await Task.Yield();

        yield return value;
    }

    private static async IAsyncEnumerable<int> FailingAfter(int count, Exception failure)
    {
        for (var i = 0; i < count; i++)
        {
            yield return i;

            await Task.Yield();
        }

        throw failure;
    }

    private sealed class Tracked
    {
        private int _opened;
        private int _closed;

        public bool IsClosed => Volatile.Read(ref _closed) > 0;

        public int OpenCount => Volatile.Read(ref _opened) - Volatile.Read(ref _closed);

        public async IAsyncEnumerable<int> Endless()
        {
            Interlocked.Increment(ref _opened);

            try
            {
                for (var i = 0; ; i++)
                {
                    yield return i;

                    await Task.Yield();
                }
            }
            finally
            {
                Interlocked.Increment(ref _closed);
            }
        }
    }
}
