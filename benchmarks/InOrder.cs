using BenchmarkDotNet.Attributes;

namespace Snail.Toolkit.Async.Pipeline.Benchmarks;

/// <summary>
/// The per-element cost of the operators that run concurrently and still answer in source order: the parallel
/// <c>Select</c> and <c>Where</c>.
/// </summary>
/// <remarks>
/// <para>
/// Ten thousand elements at a degree of four, with a selector that completes at once and one that yields
/// before it answers: what each element costs is what the operator itself allocates, since neither the source
/// nor the selector allocates per element when it completes at once.
/// </para>
/// <para>
/// The setup runs both operators once and checks the answers, so a change to the operators that is timed here
/// cannot also change what they return.
/// </para>
/// </remarks>
[MemoryDiagnoser]
public class InOrder
{
    private const int Length = 10_000;
    private const int Degree = 4;

    /// <summary>Whether the selector yields before it answers, as an awaited call would.</summary>
    [Params(false, true)]
    public bool Yields { get; set; }

    /// <summary>Checks that both operators answer every element, in source order.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var doubled = await Collect(Source().ToPipeline().Select(Doubled, Degree)).ConfigureAwait(false);
        var even = await Collect(Source().ToPipeline().Where(IsEven, Degree)).ConfigureAwait(false);

        if (!doubled.SequenceEqual(Enumerable.Range(0, Length).Select(value => value * 2)))
            throw new InvalidOperationException("The ordered Select answered other values or another order.");

        if (!even.SequenceEqual(Enumerable.Range(0, Length).Where(value => value % 2 == 0)))
            throw new InvalidOperationException("The parallel Where answered other values or another order.");
    }

    /// <summary>Projects every element and reads the results in source order.</summary>
    [Benchmark]
    public async Task<long> Select()
    {
        long total = 0;

        await foreach (var value in Source().ToPipeline().Select(Doubled, Degree).ConfigureAwait(false))
            total += value;

        return total;
    }

    /// <summary>Tests every element and reads the matches in source order.</summary>
    [Benchmark]
    public async Task<long> Where()
    {
        long total = 0;

        await foreach (var value in Source().ToPipeline().Where(IsEven, Degree).ConfigureAwait(false))
            total += value;

        return total;
    }

    private static async ValueTask<List<int>> Collect(AsyncPipeline<int> pipeline)
    {
        List<int> values = [];

        await foreach (var value in pipeline.ConfigureAwait(false))
            values.Add(value);

        return values;
    }

    private static async IAsyncEnumerable<int> Source()
    {
        for (var i = 0; i < Length; i++)
            yield return i;

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private ValueTask<int> Doubled(int value, CancellationToken cancellationToken)
        => Yields ? DoubledLaterAsync(value) : ValueTask.FromResult(value * 2);

    private ValueTask<bool> IsEven(int value, CancellationToken cancellationToken)
        => Yields ? IsEvenLaterAsync(value) : ValueTask.FromResult(value % 2 == 0);

    private static async ValueTask<int> DoubledLaterAsync(int value)
    {
        await Task.Yield();

        return value * 2;
    }

    private static async ValueTask<bool> IsEvenLaterAsync(int value)
    {
        await Task.Yield();

        return value % 2 == 0;
    }
}
