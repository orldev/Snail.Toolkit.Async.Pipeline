namespace Snail.Toolkit.Async.Pipeline;

public sealed partial class AsyncPipeline<T>
{
    /// <summary>Folds the sequence into one value.</summary>
    /// <exception cref="InvalidOperationException">The sequence is empty.</exception>
    public ValueTask<T> AggregateAsync(Func<T, T, T> func, CancellationToken cancellationToken = default)
        => source.AggregateAsync(func, cancellationToken);

    /// <summary>Folds the sequence into one value, starting from <paramref name="seed"/>.</summary>
    public ValueTask<TResult> AggregateAsync<TResult>(
        TResult seed,
        Func<TResult, T, TResult> func,
        CancellationToken cancellationToken = default)
        => source.AggregateAsync(seed, func, cancellationToken);

    /// <summary>Reads the whole sequence into a list.</summary>
    /// <remarks>Holds every element in memory, so it answers only for a sequence known to be finite.</remarks>
    public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default)
        => source.ToListAsync(cancellationToken);

    /// <summary>Reads the whole sequence into an array.</summary>
    /// <remarks>Holds every element in memory, so it answers only for a sequence known to be finite.</remarks>
    public ValueTask<T[]> ToArrayAsync(CancellationToken cancellationToken = default)
        => source.ToArrayAsync(cancellationToken);

    /// <summary>Reads the whole sequence into a dictionary keyed by <paramref name="keySelector"/>.</summary>
    /// <exception cref="ArgumentException">Two elements produce the same key.</exception>
    public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
        Func<T, TKey> keySelector,
        CancellationToken cancellationToken = default) where TKey : notnull
        => source.ToDictionaryAsync(keySelector, cancellationToken: cancellationToken);

    /// <summary>Reads the first element and stops.</summary>
    /// <exception cref="InvalidOperationException">The sequence is empty.</exception>
    public ValueTask<T> FirstAsync(CancellationToken cancellationToken = default)
        => source.FirstAsync(cancellationToken);

    /// <summary>Reads the first element, or the type default when there is none.</summary>
    /// <remarks>
    /// A default result does not distinguish an empty sequence from a first element that is itself the
    /// default; use <see cref="AnyAsync"/> when the difference matters.
    /// </remarks>
    public ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
        => source.FirstOrDefaultAsync(cancellationToken);

    /// <summary>Answers whether the sequence holds anything, reading at most one element.</summary>
    public ValueTask<bool> AnyAsync(CancellationToken cancellationToken = default)
        => source.AnyAsync(cancellationToken);

    /// <summary>Counts the elements, reading all of them.</summary>
    public ValueTask<int> CountAsync(CancellationToken cancellationToken = default)
        => source.CountAsync(cancellationToken);
}
