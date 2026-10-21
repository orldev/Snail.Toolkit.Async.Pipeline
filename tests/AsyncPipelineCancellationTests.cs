namespace Snail.Toolkit.Async.Pipeline.Tests;

/// <summary>
/// What the consumer's token buys, with selectors and sources that behave as real I/O does: ones that honour the token
/// they are handed, and ones that ignore it.
/// </summary>
public class AsyncPipelineCancellationTests
{
    /// <summary>
    /// A caller tells its own cancellation from a timeout inside the operation by the token on the exception, as
    /// HttpClient's callers do. A selector that honours the token it was given throws with the enumeration's linked
    /// token; the consumer must still receive its own. The stopping tests pass a selector that never awaits, which
    /// is why this case needs a test of its own.
    /// </summary>
    [Fact]
    public async Task Select_WhenTheCallerCancelsASelectorThatHonoursTheToken_ThrowsWithTheCallersToken()
    {
        using var cancellation = new CancellationTokenSource();
        var entered = 0;

        var pipeline = Enumerable.Range(0, 10).ToPipeline().Select(
            async (x, token) =>
            {
                Interlocked.Increment(ref entered);
                await Task.Delay(Timeout.Infinite, token);

                return x;
            },
            2);

        var enumerator = pipeline.GetAsyncEnumerator(cancellation.Token);
        var reading = enumerator.MoveNextAsync().AsTask();

        while (Volatile.Read(ref entered) == 0)
            await Task.Yield();

        await Task.Delay(50);
        await cancellation.CancelAsync();

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reading);
        await enumerator.DisposeAsync();

        Assert.Equal(cancellation.Token, thrown.CancellationToken);
    }

    /// <summary>
    /// The documented limit of "no work outlives the consumer": leaving the loop waits for the pump, and the pump
    /// waits in the source's MoveNextAsync, which nothing can interrupt. A source that ignores its token holds the
    /// consumer's DisposeAsync until it answers, and releases it the moment it does.
    /// </summary>
    [Fact]
    public async Task Select_WhenTheSourceIgnoresCancellation_DisposeWaitsUntilItAnswers()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Silent(gate.Task).ToPipeline().Select((x, _) => ValueTask.FromResult(x), 2);

        var enumerator = pipeline.GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());

        var disposing = enumerator.DisposeAsync().AsTask();
        var leftWhileSilent = await Task.WhenAny(disposing, Task.Delay(200)) == disposing;

        gate.SetResult();
        await disposing.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.False(leftWhileSilent, "the enumeration returned while its source was still inside MoveNextAsync");
    }

    private static async IAsyncEnumerable<int> Silent(Task answer)
    {
        yield return 0;

        await answer;
    }
}
