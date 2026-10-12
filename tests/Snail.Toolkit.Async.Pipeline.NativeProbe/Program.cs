using Snail.Toolkit.Async.Pipeline;

var numbers = Enumerable.Range(1, 1_000).ToArray();

var squares = await numbers.ToPipeline()
    .Select((number, _) => ValueTask.FromResult(number * number), 4)
    .ToArrayAsync();

var evens = await numbers.ToPipeline()
    .Where(async (number, token) =>
    {
        await Task.Yield();

        return number % 2 == 0;
    }, 4)
    .CountAsync();

var unordered = await numbers.ToPipeline()
    .SelectUnordered((number, _) => ValueTask.FromResult(number), 4)
    .AggregateAsync(0L, (sum, number) => sum + number);

var groups = await AsyncPipelineBuilder.Merge(numbers.ToAsyncEnumerable(), numbers.ToAsyncEnumerable())
    .GroupBy(number => number % 3)
    .CountAsync();

var isOrdered = squares.SequenceEqual(numbers.Select(number => number * number));
var isWorking = isOrdered && evens == 500 && unordered == 500_500 && groups == 3;

Console.WriteLine($"ordered {isOrdered}, evens {evens}, sum {unordered}, groups {groups}");

return isWorking ? 0 : 1;
