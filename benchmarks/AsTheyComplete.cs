using BenchmarkDotNet.Attributes;

namespace Snail.Toolkit.Async.Pipeline.Benchmarks;

/// <summary>
/// The per-element cost of the operators that answer in completion order: <c>SelectUnordered</c>, the parallel
/// <c>SelectMany</c> and <c>Merge</c>.
/// </summary>
/// <remarks>
/// <para>
/// Ten thousand elements at a degree of four; <c>SelectMany</c> expands each into two, and <c>Merge</c> reads
/// four sources of a quarter each. Each runs with calls (or sources) that complete at once and with ones that
/// yield first.
/// </para>
/// <para>
/// The order is free, so the setup checks every operator's answers as a sorted list: a change timed here cannot
/// lose, repeat or alter an element.
/// </para>
/// </remarks>
[MemoryDiagnoser]
public class AsTheyComplete
{
    private const int Length = 10_000;
    private const int Degree = 4;
    private const int Sources = 4;

    /// <summary>Whether each call, sub-sequence or merged source yields before it answers.</summary>
    [Params(false, true)]
    public bool Yields { get; set; }

    /// <summary>Checks that every operator answers each element exactly once.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var doubled = await Collect(Unordered()).ConfigureAwait(false);
        var expanded = await Collect(Expanded()).ConfigureAwait(false);
        var merged = await Collect(Merged()).ConfigureAwait(false);

        var everyElement = Enumerable.Range(0, Length).ToArray();

        if (!doubled.Order().SequenceEqual(everyElement.Select(value => value * 2)))
            throw new InvalidOperationException("SelectUnordered lost, repeated or altered an element.");

        if (!expanded.Order().SequenceEqual(everyElement.SelectMany(value => new[] { value, value })))
            throw new InvalidOperationException("The parallel SelectMany lost, repeated or altered an element.");

        if (!merged.Order().SequenceEqual(everyElement.Select(value => value % (Length / Sources)).Order()))
            throw new InvalidOperationException("Merge lost, repeated or altered an element.");
    }

    /// <summary>Projects every element and reads the results as they complete.</summary>
    [Benchmark]
    public Task<long> SelectUnordered() => Sum(Unordered());

    /// <summary>Expands every element into two and reads them as they come.</summary>
    [Benchmark]
    public Task<long> SelectMany() => Sum(Expanded());

    /// <summary>Reads four sources at once.</summary>
    [Benchmark]
    public Task<long> Merge() => Sum(Merged());

    private AsyncPipeline<int> Unordered()
        => Source(Length, false).ToPipeline().SelectUnordered(Doubled, Degree);

    private AsyncPipeline<int> Expanded()
        => Source(Length, false).ToPipeline().SelectMany((value, _) => Twice(value), Degree);

    private AsyncPipeline<int> Merged()
    {
        var sources = new IAsyncEnumerable<int>[Sources];
        for (var i = 0; i < Sources; i++)
            sources[i] = Source(Length / Sources, Yields);

        return AsyncPipelineBuilder.Merge(sources);
    }

    private static async Task<long> Sum(AsyncPipeline<int> pipeline)
    {
        long total = 0;

        await foreach (var value in pipeline.ConfigureAwait(false))
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

    private static async IAsyncEnumerable<int> Source(int length, bool yields)
    {
        for (var i = 0; i < length; i++)
        {
            yield return i;

            if (yields)
                await Task.Yield();
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private async IAsyncEnumerable<int> Twice(int value)
    {
        yield return value;

        if (Yields)
            await Task.Yield();

        yield return value;
    }

    private ValueTask<int> Doubled(int value, CancellationToken cancellationToken)
        => Yields ? DoubledLaterAsync(value) : ValueTask.FromResult(value * 2);

    private static async ValueTask<int> DoubledLaterAsync(int value)
    {
        await Task.Yield();

        return value * 2;
    }
}
