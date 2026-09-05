namespace Snail.Toolkit.Async.Pipeline;

/// <summary>
/// Turns ordinary collections into an <see cref="AsyncPipeline{T}"/>.
/// </summary>
public static class AsyncPipelineExtensions
{
    extension<T>(IAsyncEnumerable<T> source)
    {
        /// <summary>Wraps the sequence in a pipeline.</summary>
        /// <exception cref="ArgumentNullException">The sequence is null.</exception>
        public AsyncPipeline<T> ToPipeline()
        {
            ArgumentNullException.ThrowIfNull(source);

            return new AsyncPipeline<T>(source);
        }
    }

    extension<T>(IEnumerable<T> source)
    {
        /// <summary>Wraps the collection in a pipeline that yields its elements asynchronously.</summary>
        /// <exception cref="ArgumentNullException">The collection is null.</exception>
        public AsyncPipeline<T> ToPipeline()
        {
            ArgumentNullException.ThrowIfNull(source);

            return new AsyncPipeline<T>(source.ToAsyncEnumerable());
        }
    }
}
