namespace Snail.Toolkit.Async.Pipeline.Tests;

public class AsyncPipelineTests
{
    [Fact]
    public async Task Pipeline_WhenOperatorsAreChained_AppliesThemInOrder()
    {
        var pipeline = AsyncPipelineBuilder.From(Enumerable.Range(1, 10))
            .Select(x => x * 10)
            .Where(x => x > 50)
            .Skip(1)
            .Take(2);

        var result = await pipeline.ToListAsync();

        Assert.Equal([70, 80], result);
    }

    [Fact]
    public async Task Merge_WhenSourcesAreCombined_YieldsEveryElement()
    {
        var pipeline = AsyncPipelineBuilder.Merge(
                AsyncPipelineBuilder.From([1, 2, 3]).AsAsyncEnumerable(),
                AsyncPipelineBuilder.From([4, 5, 6]).AsAsyncEnumerable())
            .Select(x => x * 2);

        var result = await pipeline.ToListAsync();

        Assert.Equal([2, 4, 6, 8, 10, 12], [.. result.Order()]);
    }

    [Fact]
    public async Task Select_WhenSelectorThrows_PropagatesToConsumer()
    {
        var pipeline = AsyncPipelineBuilder.From([1, 2, 3])
            .Select(x => x == 2 ? throw new InvalidOperationException("no") : x);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipeline.ToListAsync());
    }

    [Fact]
    public async Task SelectMany_WhenSelectorReturnsCollections_FlattensThem()
    {
        var pipeline = AsyncPipelineBuilder.From(["abc", "de"])
            .SelectMany(s => s.ToCharArray());

        var result = await pipeline.ToListAsync();

        Assert.Equal(['a', 'b', 'c', 'd', 'e'], result);
    }

    [Fact]
    public async Task SelectMany_WhenSubStreamsAreNested_KeepsSourceOrder()
    {
        var pipeline = AsyncPipelineBuilder.From(Categories()).SelectMany(Items);

        var result = await pipeline.ToListAsync();

        Assert.Equal(["Item 1-A", "Item 1-B", "Item 2-A", "Item 2-B"], result);
        return;

        static async IAsyncEnumerable<int> Categories()
        {
            yield return 1;
            await Task.Yield();
            yield return 2;
        }

        static async IAsyncEnumerable<string> Items(int category)
        {
            await Task.Yield();
            yield return $"Item {category}-A";
            yield return $"Item {category}-B";
        }
    }

    /// <summary>
    /// A pipeline is a description of work, so each enumeration runs the factory again. The overload
    /// takes a factory precisely so that this is legal: a ValueTask may be consumed only once.
    /// </summary>
    [Fact]
    public async Task From_WhenFactoryIsEnumeratedTwice_ProducesValueTwice()
    {
        var calls = 0;
        var pipeline = AsyncPipelineBuilder.From(() =>
        {
            calls++;
            return ValueTask.FromResult(calls);
        });

        var first = await pipeline.ToListAsync();
        var second = await pipeline.ToListAsync();

        Assert.Equal([1], first);
        Assert.Equal([2], second);
    }

    /// <summary>
    /// The overload exists to hand the enumeration's own token to the factory, so a factory holding it
    /// must see the cancellation the consumer asks for. Handed the wrong token it would look identical
    /// until something needed cancelling.
    /// </summary>
    [Fact]
    public async Task From_WhenFactoryTakesAToken_ReceivesTheEnumerationToken()
    {
        using var enumeration = new CancellationTokenSource();
        var given = CancellationToken.None;

        var pipeline = AsyncPipelineBuilder.From(token =>
        {
            given = token;
            return ValueTask.FromResult(1);
        });

        var enumerator = pipeline.GetAsyncEnumerator(enumeration.Token);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal(1, enumerator.Current);
        Assert.False(given.IsCancellationRequested);

        await enumeration.CancelAsync();

        Assert.True(given.IsCancellationRequested);

        await enumerator.DisposeAsync();
    }

    [Fact]
    public void From_WhenSourceIsNull_ThrowsBeforeEnumeration()
        => Assert.Throws<ArgumentNullException>(() => AsyncPipelineBuilder.From((IEnumerable<int>)null!));

    [Fact]
    public async Task Merge_WhenOneSourceFails_PropagatesFailure()
    {
        var pipeline = AsyncPipelineBuilder.Merge(Failing(), Endless());

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipeline.ToListAsync());
        return;

        static async IAsyncEnumerable<int> Failing()
        {
            await Task.Yield();
            throw new InvalidOperationException("source is gone");
#pragma warning disable CS0162
            yield break;
#pragma warning restore CS0162
        }

        static async IAsyncEnumerable<int> Endless()
        {
            while (true)
            {
                yield return 1;
                await Task.Yield();
            }
        }
    }

    [Fact]
    public async Task Merge_WhenTokenIsCancelled_StopsEnumerating()
    {
        var pipeline = AsyncPipelineBuilder.Merge(Endless());
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in pipeline.WithCancellation(cancelled.Token))
            {
            }
        });

        return;

        static async IAsyncEnumerable<int> Endless()
        {
            while (true)
            {
                yield return 1;
                await Task.Yield();
            }
        }
    }

    /// <summary>
    /// The contract every operator here owes its source: leaving the loop early closes it. Without this
    /// a pipeline abandoned mid-stream leaks the connection behind the source.
    /// </summary>
    [Fact]
    public async Task Merge_WhenConsumerBreaksEarly_DisposesEverySource()
    {
        var closed = 0;
        var pipeline = AsyncPipelineBuilder.Merge(Tracked(), Tracked());

        await foreach (var _ in pipeline)
            break;

        Assert.Equal(2, closed);
        return;

        async IAsyncEnumerable<int> Tracked()
        {
            try
            {
                while (true)
                {
                    yield return 1;
                    await Task.Yield();
                }
            }
            finally
            {
                Interlocked.Increment(ref closed);
            }
        }
    }
}
