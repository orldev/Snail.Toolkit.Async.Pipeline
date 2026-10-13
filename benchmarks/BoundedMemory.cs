using BenchmarkDotNet.Attributes;

namespace Snail.Toolkit.Async.Pipeline.Benchmarks;

/// <summary>
/// Measures the one promise this library makes that no test states as a number: memory bounded by the
/// degree of parallelism rather than by the length of the source.
/// </summary>
/// <remarks>
/// <para>
/// Read the Allocated column down, not the Mean column across. <see cref="AbandonedAfterAPrefix"/> reads
/// a fixed prefix and leaves; its allocation must stay flat as <see cref="SourceLength"/> grows tenfold,
/// because the results channel holds at most <see cref="Degree"/> of them and a consumer that stops
/// reading stops the source. <see cref="DrainedToTheEnd"/> consumes everything, so it may grow only by
/// what the selector allocates; this selector answers at once and the operator allocates nothing per
/// element, so it stays flat as well. A line that grows with the source is the bound broken.
/// </para>
/// <para>
/// The defect this guards against measured 100000 elements read out of a 100000-element source at a
/// degree of four: a semaphore bounds the calls in flight and lets completed results pile up behind a
/// slow consumer, so allocation tracked the source instead of the degree.
/// </para>
/// </remarks>
[MemoryDiagnoser]
public class BoundedMemory
{
    private const int Degree = 4;
    private const int Prefix = 100;

    /// <summary>Length of the source the pipeline is offered, only part of which may be read.</summary>
    [Params(10_000, 100_000)]
    public int SourceLength { get; set; }

    /// <summary>Reads a fixed prefix and abandons the pipeline, as a consumer that gave up would.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> AbandonedAfterAPrefix()
    {
        var read = 0;

        await foreach (var _ in Counted(SourceLength).ToPipeline().Select(Echo, Degree).ConfigureAwait(false))
        {
            if (++read == Prefix)
                break;
        }

        return read;
    }

    /// <summary>Reads every element, where allocation is expected to grow with the source.</summary>
    [Benchmark]
    public async Task<int> DrainedToTheEnd()
    {
        var read = 0;

        await foreach (var _ in Counted(SourceLength).ToPipeline().Select(Echo, Degree).ConfigureAwait(false))
            read++;

        return read;
    }

    private static ValueTask<int> Echo(int value, CancellationToken cancellationToken)
        => ValueTask.FromResult(value);

    private static async IAsyncEnumerable<int> Counted(int length)
    {
        for (var i = 0; i < length; i++)
        {
            yield return i;

            await Task.Yield();
        }
    }
}
