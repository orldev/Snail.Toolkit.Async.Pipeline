# Snail.Toolkit.Async.Pipeline

`AsyncPipeline<T>` is a fluent wrapper around `IAsyncEnumerable<T>`: LINQ-like operators, and parallel
ones that bound both their concurrency and their memory.

---

## Entry points

```csharp
var fromAsync = myAsyncSource.ToPipeline();
var fromList  = myList.ToPipeline();
var fromOne   = AsyncPipelineBuilder.From(() => LoadAsync());
var merged    = AsyncPipelineBuilder.Merge(first, second);
```

`From` takes a factory rather than a `ValueTask<T>`: a pipeline may be enumerated more than once, and a
`ValueTask` may be consumed only once.

## Parallel operators

Every parallel overload requires `maxDegreeOfParallelism`. There is no default, because a default of one
turns a parallel operator into a slower sequential one without saying so.

```csharp
// Ordered: results come out in source order.
var ordered = pipeline.Select(FetchAsync, maxDegreeOfParallelism: 8);

// Unordered: results come out as they finish.
var fastest = pipeline.SelectUnordered(FetchAsync, maxDegreeOfParallelism: 8);

// Concurrent filter, source order kept.
var live = pipeline.Where(IsReachableAsync, maxDegreeOfParallelism: 8);

// Several sub-streams read side by side, results interleaved.
var files = directories
    .ToPipeline()
    .SelectMany((path, ct) => fileService.SearchFilesAsync(path, ct), maxDegreeOfParallelism: 4);
```

The selector takes `(element, CancellationToken)`. Note that `SelectMany` takes a plain method returning
`IAsyncEnumerable<T>` — not an `async` lambda.

### What the degree actually bounds

It bounds the results held in memory, not only the calls in flight. A slow consumer stops the source
from being read, so an ordered `Select` over an endless source runs in constant memory even when one
element is slow.

### What happens when something fails

The first failure faults the sequence and stops the rest. Nothing is dropped silently — if an element's
selector throws, the consumer sees that exception rather than a quietly shorter result.

### What happens when you stop reading

Leaving the loop — by `break`, by exception or by cancellation — cancels the pending calls, disposes the
source and waits for the background work before returning. No work outlives the consumer.

## Cancellation

The token is supplied at enumeration and reaches every operator in the chain:

```csharp
await foreach (var item in pipeline.WithCancellation(ct))
{
}
```

Operators do not capture a token when the chain is built.

## Terminal methods

```csharp
List<User> users = await pipeline.ToListAsync(ct);
User? first     = await pipeline.FirstOrDefaultAsync(ct);
int total       = await pipeline.AggregateAsync(0, (sum, x) => sum + x, ct);
```

`ToListAsync`, `ToArrayAsync` and `ToDictionaryAsync` hold every element, so they answer only for a
sequence known to be finite.

## Costs worth knowing

| Operator | Cost |
| :--- | :--- |
| `Distinct` | remembers every distinct element for the whole enumeration |
| `GroupBy` | reads the source to its end before yielding the first group |
| `Merge` | order is not preserved and not stable between runs |

## Enumerating twice

A pipeline is a description of work, not a running one, and each enumeration starts the chain again.
Whether a second enumeration is legal belongs to the source: a pipeline over a network stream or a
database cursor is single-use.

## Benchmarks

The bound on memory is the one promise here that no test states as a number, so it is measured instead:

```bash
dotnet run -c Release --project benchmarks -- --filter '*'
```

`AbandonedAfterAPrefix` reads a fixed prefix and leaves. Read the `Allocated` column down: it must stay
flat as the source grows from ten thousand elements to a hundred thousand, because the results channel
holds at most `maxDegreeOfParallelism` of them and a consumer that stops reading stops the source.
`DrainedToTheEnd` beside it consumes everything: it may grow only by what the selector itself allocates,
and with the selector there, which answers at once, the operator allocates nothing per element, so it
stays flat too. A line that grows with the source is the bound broken.

`InOrder` and `AsTheyComplete` measure what each element costs the parallel operators, those that keep
the source order and those that answer as calls complete, with calls that answer at once and calls
that yield first. Each checks its operators' answers in its setup, and `benchmarks/baseline/` keeps the
last run.

## License

Released under the [MIT license](LICENSE).
