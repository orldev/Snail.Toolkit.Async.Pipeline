namespace Snail.Toolkit.Async.Pipeline.Tests;

/// <summary>
/// Covers the operators that delegate straight to the in-box implementation. They carry no logic of
/// their own, so one assertion each is the whole story — the point is that the delegation exists and
/// still binds after the surface changed.
/// </summary>
public class AsyncPipelineSurfaceTests
{
    [Fact]
    public async Task Distinct_WhenElementsRepeat_KeepsTheFirstOfEach()
        => Assert.Equal([1, 2, 3], await AsyncPipelineBuilder.From([1, 2, 2, 3, 1]).Distinct().ToListAsync());

    [Fact]
    public async Task Concat_WhenSecondSequenceIsGiven_ReadsItAfterTheFirst()
        => Assert.Equal(
            [1, 2, 3, 4],
            await AsyncPipelineBuilder.From([1, 2])
                .Concat(AsyncPipelineBuilder.From([3, 4]).AsAsyncEnumerable())
                .ToListAsync());

    [Fact]
    public async Task Merge_WhenAnotherSequenceIsGiven_YieldsBothSets()
    {
        var result = await AsyncPipelineBuilder.From([1, 2])
            .Merge(AsyncPipelineBuilder.From([3, 4]).AsAsyncEnumerable())
            .ToListAsync();

        Assert.Equal([1, 2, 3, 4], [.. result.Order()]);
    }

    [Fact]
    public async Task GroupBy_WhenKeysRepeat_GathersElementsUnderOneKey()
    {
        var groups = await AsyncPipelineBuilder.From([1, 2, 3, 4, 5])
            .GroupBy(x => x % 2 == 0)
            .ToListAsync();

        Assert.Equal(2, groups.Count);
        Assert.Equal([1, 3, 5], [.. groups.Single(g => !g.Key)]);
    }

    [Fact]
    public async Task AsyncPipelineBuilder_WhenSourcesAreConcatenated_ReadsThemInOrder()
        => Assert.Equal(
            [1, 2, 3, 4],
            await AsyncPipelineBuilder.Concat(
                    AsyncPipelineBuilder.From([1, 2]).AsAsyncEnumerable(),
                    AsyncPipelineBuilder.From([3, 4]).AsAsyncEnumerable())
                .ToListAsync());

    [Fact]
    public async Task From_WhenCollectionIsStillBeingFetched_YieldsItOnEnumeration()
        => Assert.Equal(
            [1, 2],
            await AsyncPipelineBuilder.From(Task.FromResult<IEnumerable<int>>([1, 2])).ToListAsync());

    [Fact]
    public async Task ToPipeline_WhenCalledOnACollection_WrapsIt()
        => Assert.Equal([1, 2], await new[] { 1, 2 }.ToPipeline().ToListAsync());

    [Fact]
    public async Task ToPipeline_WhenCalledOnAnAsyncSequence_WrapsIt()
        => Assert.Equal(
            [1, 2],
            await AsyncPipelineBuilder.From([1, 2]).AsAsyncEnumerable().ToPipeline().ToListAsync());

    [Fact]
    public async Task AggregateAsync_WhenNoSeedIsGiven_FoldsFromTheFirstElement()
        => Assert.Equal(6, await AsyncPipelineBuilder.From([1, 2, 3]).AggregateAsync((a, b) => a + b));

    [Fact]
    public async Task AggregateAsync_WhenSeedIsGiven_FoldsFromIt()
        => Assert.Equal("123", await AsyncPipelineBuilder.From([1, 2, 3]).AggregateAsync("", (a, b) => a + b));

    [Fact]
    public async Task ToArrayAsync_WhenSequenceIsFinite_ReadsItAll()
    {
        var read = await AsyncPipelineBuilder.From([1, 2, 3]).ToArrayAsync();

        Assert.Equal([1, 2, 3], read);
    }

    [Fact]
    public async Task ToDictionaryAsync_WhenKeysAreUnique_IndexesEveryElement()
    {
        var byKey = await AsyncPipelineBuilder.From([1, 2, 3]).ToDictionaryAsync(x => $"k{x}");

        Assert.Equal(3, byKey.Count);
        Assert.Equal(2, byKey["k2"]);
    }

    [Fact]
    public async Task FirstAsync_WhenSequenceIsEmpty_Throws()
        => await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await AsyncPipelineBuilder.From(Array.Empty<int>()).FirstAsync());

    [Fact]
    public async Task FirstAsync_WhenSequenceHasElements_ReadsTheFirst()
        => Assert.Equal(1, await AsyncPipelineBuilder.From([1, 2]).FirstAsync());

    [Fact]
    public async Task FirstOrDefaultAsync_WhenSequenceIsEmpty_AnswersTheDefault()
        => Assert.Equal(0, await AsyncPipelineBuilder.From(Array.Empty<int>()).FirstOrDefaultAsync());

    [Fact]
    public async Task AnyAsync_WhenSequenceIsEmpty_AnswersFalse()
        => Assert.False(await AsyncPipelineBuilder.From(Array.Empty<int>()).AnyAsync());

    [Fact]
    public async Task CountAsync_WhenSequenceHasElements_CountsThem()
        => Assert.Equal(3, await AsyncPipelineBuilder.From([1, 2, 3]).CountAsync());
}
