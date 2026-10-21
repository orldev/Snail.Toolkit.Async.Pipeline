namespace Snail.Toolkit.Async.Pipeline.Tests;

/// <summary>
/// The promise the degree makes, taken literally: no more calls than the degree at once, no call left running after
/// the consumer has gone, and work that follows the elements rather than the degree.
/// </summary>
public class AsyncPipelineBoundsTests
{
    /// <summary>
    /// The defect this replaces: the ordered pump started a call on the next element before it waited for a free
    /// slot, and the consumer took a call out of the channel before awaiting it, so a degree of one ran three calls at
    /// once — which a selector that must be serialised, a client with a rate limit, noticed.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Select_WhenCallsOverlap_RunsNoMoreThanTheDegreeAtOnce(int degree)
    {
        var calls = new Overlap();

        await Enumerable.Range(0, 32).ToPipeline()
            .Select((x, _) => calls.RunAsync(x), degree)
            .ToListAsync();

        Assert.True(calls.Peak <= degree, $"{calls.Peak} calls ran at once at a degree of {degree}");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Where_WhenCallsOverlap_RunsNoMoreThanTheDegreeAtOnce(int degree)
    {
        var calls = new Overlap();

        await Enumerable.Range(0, 32).ToPipeline()
            .Where((x, _) => calls.IsEvenAsync(x), degree)
            .ToListAsync();

        Assert.True(calls.Peak <= degree, $"{calls.Peak} calls ran at once at a degree of {degree}");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task SelectUnordered_WhenCallsOverlap_RunsNoMoreThanTheDegreeAtOnce(int degree)
    {
        var calls = new Overlap();

        await Enumerable.Range(0, 32).ToPipeline()
            .SelectUnordered((x, _) => calls.RunAsync(x), degree)
            .ToListAsync();

        Assert.True(calls.Peak <= degree, $"{calls.Peak} calls ran at once at a degree of {degree}");
    }

    /// <summary>
    /// "No work outlives the consumer": leaving the loop awaits every call that was started. The pump used to hold a
    /// call it had started while it was blocked on a full channel, and the cleanup awaited the channel and the pump
    /// but never that call, so one selector per abandoned enumeration kept running after the enumeration returned.
    /// </summary>
    [Fact]
    public async Task Select_WhenConsumerLeaves_AwaitsEveryCallThatWasStarted()
    {
        var gates = Gates(3);
        var started = 0;
        var finished = 0;

        var pipeline = new[] { 0, 1, 2 }.ToPipeline().Select(
            async (x, _) =>
            {
                Interlocked.Increment(ref started);
                var value = x == 0 ? 0 : await gates[x].Task;
                Interlocked.Increment(ref finished);

                return value;
            },
            maxDegreeOfParallelism: 1);

        var enumerator = pipeline.GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        await SettleAsync(() => Volatile.Read(ref started) == 3);

        var disposing = enumerator.DisposeAsync().AsTask();
        gates[1].SetResult(1);
        await disposing;

        var stillRunning = Volatile.Read(ref started) - Volatile.Read(ref finished);
        gates[2].SetResult(2);

        Assert.Equal(0, stillRunning);
    }

    /// <summary>
    /// A worker is started as an element needs one, so a degree far above the source — a thousand for three files —
    /// costs about what a degree of ten does. A worker per degree started up front cost 545 bytes each.
    /// </summary>
    [Fact]
    public async Task SelectUnordered_WhenDegreeFarExceedsTheSource_AllocatesForTheElementsNotTheDegree()
    {
        var small = await AllocatedAsync(() => Three().SelectUnordered(Echo, 10).ToListAsync());
        var large = await AllocatedAsync(() => Three().SelectUnordered(Echo, 10_000).ToListAsync());

        Assert.True(
            large < 4 * small,
            $"{large:N0} bytes at a degree of 10000 against {small:N0} at a degree of 10, for three elements");
    }

    /// <summary>
    /// The validation accepts any degree of one or more. The unordered operators used to size a worker array at
    /// degree plus one, which overflowed at int.MaxValue, faulted the runner before it completed the results channel,
    /// and left the consumer waiting on that channel for ever.
    /// </summary>
    [Fact]
    public async Task SelectUnordered_WhenDegreeIsIntMaxValue_Answers()
    {
        var pipeline = Three().SelectUnordered(Echo, int.MaxValue);

        var answers = await pipeline.ToListAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal([0, 1, 2], answers.Order());
    }

    /// <summary>A pipeline is a description of work, so many enumerations at once share no state.</summary>
    [Fact]
    public async Task SelectUnordered_WhenSixtyFourEnumerationsRunAtOnce_EachAnswersItsOwnElements()
    {
        var pipeline = Enumerable.Range(0, 500).ToPipeline().SelectUnordered(LaterAsync, 8);

        var runs = Enumerable.Range(0, 64).Select(_ => pipeline.ToListAsync().AsTask());
        var answers = await Task.WhenAll(runs);

        Assert.All(answers, answer => Assert.Equal(Enumerable.Range(0, 500), answer.Order()));
    }

    private static AsyncPipeline<int> Three() => new[] { 0, 1, 2 }.ToPipeline();

    private static ValueTask<int> Echo(int value, CancellationToken cancellationToken) => ValueTask.FromResult(value);

    private static async ValueTask<int> LaterAsync(int value, CancellationToken cancellationToken)
    {
        await Task.Yield();

        return value;
    }

    private static TaskCompletionSource<int>[] Gates(int count) =>
        [.. Enumerable.Range(0, count)
            .Select(_ => new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously))];

    private static async Task SettleAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(300);

        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(1);
    }

    private static async Task<long> AllocatedAsync(Func<ValueTask<List<int>>> run)
    {
        await run();

        var before = GC.GetTotalAllocatedBytes(precise: true);
        await run();

        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }

    private sealed class Overlap
    {
        private int _inFlight;
        private int _peak;

        public int Peak => Volatile.Read(ref _peak);

        public async ValueTask<int> RunAsync(int value)
        {
            Enter();
            await Task.Delay(10);
            Interlocked.Decrement(ref _inFlight);

            return value;
        }

        public async ValueTask<bool> IsEvenAsync(int value)
        {
            var answered = await RunAsync(value);

            return answered % 2 == 0;
        }

        private void Enter()
        {
            var now = Interlocked.Increment(ref _inFlight);
            int seen;

            do
            {
                seen = Volatile.Read(ref _peak);

                if (now <= seen)
                    return;
            }
            while (Interlocked.CompareExchange(ref _peak, now, seen) != seen);
        }
    }
}
