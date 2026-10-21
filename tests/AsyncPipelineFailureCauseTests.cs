namespace Snail.Toolkit.Async.Pipeline.Tests;

/// <summary>
/// "The first failure wins" and "nothing is dropped silently", with the failure an I/O library meets most: a timeout,
/// which HttpClient reports as a TaskCanceledException carrying a TimeoutException, when nobody asked for cancellation.
/// </summary>
public class AsyncPipelineFailureCauseTests
{
    private static readonly string TimeoutMessage =
        "The request was canceled due to the configured HttpClient.Timeout of 100 seconds elapsing.";

    /// <summary>
    /// The defect this replaces: the fan-out told cause from consequence by exception type, so a selector's own
    /// timeout reached the consumer as a bare OperationCanceledException with no message and no cause — which a loop
    /// that expects to be cancelled on shutdown swallowed without a word.
    /// </summary>
    [Fact]
    public async Task SelectUnordered_WhenASelectorTimesOutOnItsOwn_ReportsTheTimeoutItself()
    {
        var timeout = new TaskCanceledException(TimeoutMessage, new TimeoutException());

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await Enumerable.Range(0, 50).ToPipeline()
                .SelectUnordered<int>(
                    async (x, _) =>
                    {
                        await Task.Yield();

                        return x == 3 ? throw timeout : x;
                    },
                    4)
                .ToListAsync());

        Assert.Same(timeout, thrown);
    }

    [Fact]
    public async Task SelectMany_WhenASubSequenceTimesOutOnItsOwn_ReportsTheTimeoutItself()
    {
        var timeout = new TaskCanceledException(TimeoutMessage, new TimeoutException());

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await Enumerable.Range(0, 50).ToPipeline()
                .SelectMany((x, _) => x == 3 ? TimingOut(timeout) : Twice(x), 4)
                .ToListAsync());

        Assert.Same(timeout, thrown);
    }

    [Fact]
    public async Task Merge_WhenASourceTimesOutOnItsOwn_ReportsTheTimeoutItself()
    {
        var timeout = new TaskCanceledException(TimeoutMessage, new TimeoutException());

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await AsyncPipelineBuilder.Merge(TimingOut(timeout), Endless()).ToListAsync());

        Assert.Same(timeout, thrown);
    }

    [Fact]
    public async Task Select_WhenASelectorTimesOutOnItsOwn_ReportsTheTimeoutItself()
    {
        var timeout = new TaskCanceledException(TimeoutMessage, new TimeoutException());

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await Enumerable.Range(0, 50).ToPipeline()
                .Select<int>(
                    async (x, _) =>
                    {
                        await Task.Yield();

                        return x == 3 ? throw timeout : x;
                    },
                    4)
                .ToListAsync());

        Assert.Same(timeout, thrown);
    }

    [Fact]
    public async Task Select_WhenTheSourceTimesOutOnItsOwn_ReportsTheTimeoutItself()
    {
        var timeout = new TaskCanceledException(TimeoutMessage, new TimeoutException());

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await TimingOut(timeout).ToPipeline().Select((x, _) => ValueTask.FromResult(x), 4).ToListAsync());

        Assert.Same(timeout, thrown);
    }

    private static async IAsyncEnumerable<int> TimingOut(Exception timeout)
    {
        yield return 1;

        await Task.Yield();

        throw timeout;
    }

    private static async IAsyncEnumerable<int> Twice(int value)
    {
        yield return value;

        await Task.Yield();

        yield return value;
    }

    private static async IAsyncEnumerable<int> Endless()
    {
        for (var i = 0; ; i++)
        {
            yield return i;

            await Task.Yield();
        }
    }
}
