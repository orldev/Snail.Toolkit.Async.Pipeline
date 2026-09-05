namespace Snail.Toolkit.Async.Pipeline;

public sealed partial class AsyncPipeline<T>
{
    /// <summary>Keeps the elements the predicate accepts.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is null.</exception>
    public AsyncPipeline<T> Where(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return new AsyncPipeline<T>(source.Where(predicate));
    }

    /// <summary>Turns each element into another.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is null.</exception>
    public AsyncPipeline<TResult> Select<TResult>(Func<T, TResult> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return new AsyncPipeline<TResult>(source.Select(selector));
    }

    /// <summary>Expands each element into a collection and flattens the result.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is null.</exception>
    public AsyncPipeline<TResult> SelectMany<TResult>(Func<T, IEnumerable<TResult>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return new AsyncPipeline<TResult>(source.SelectMany(selector));
    }

    /// <summary>Expands each element into a sub-sequence and flattens the result, one element at a time.</summary>
    /// <remarks>
    /// Sub-sequences are read one after another, so the result keeps the source order. Use the overload
    /// taking a degree of parallelism to read several of them side by side.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> is null.</exception>
    public AsyncPipeline<TResult> SelectMany<TResult>(Func<T, IAsyncEnumerable<TResult>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return new AsyncPipeline<TResult>(source.SelectMany(selector));
    }

    /// <summary>Drops the first <paramref name="count"/> elements.</summary>
    public AsyncPipeline<T> Skip(int count)
        => new(source.Skip(count));

    /// <summary>Keeps the first <paramref name="count"/> elements and stops reading the source.</summary>
    public AsyncPipeline<T> Take(int count)
        => new(source.Take(count));

    /// <summary>Drops elements already seen.</summary>
    /// <remarks>
    /// Remembers every distinct element for as long as the enumeration lasts, so memory grows with the
    /// number of distinct values. Over an endless source it grows without limit.
    /// </remarks>
    public AsyncPipeline<T> Distinct()
        => new(source.Distinct());

    /// <summary>Reads this sequence, then <paramref name="other"/>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="other"/> is null.</exception>
    public AsyncPipeline<T> Concat(IAsyncEnumerable<T> other)
    {
        ArgumentNullException.ThrowIfNull(other);

        return new AsyncPipeline<T>(source.Concat(other));
    }

    /// <summary>Reads this sequence and <paramref name="other"/> at once, yielding elements as they arrive.</summary>
    /// <remarks>Order is not preserved, and it is not stable between runs.</remarks>
    /// <exception cref="ArgumentNullException"><paramref name="other"/> is null.</exception>
    public AsyncPipeline<T> Merge(IAsyncEnumerable<T> other)
    {
        ArgumentNullException.ThrowIfNull(other);

        return new AsyncPipeline<T>(AsyncPipelineBuilder.Merge(source, other).AsAsyncEnumerable());
    }

    /// <summary>Gathers the elements into groups sharing a key.</summary>
    /// <remarks>
    /// Reads the source to its end before yielding the first group, because a group is not complete
    /// until the last element has been seen. Over an endless source it never yields.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is null.</exception>
    public AsyncPipeline<IGrouping<TKey, T>> GroupBy<TKey>(Func<T, TKey> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);

        return new AsyncPipeline<IGrouping<TKey, T>>(source.GroupBy(keySelector));
    }
}
