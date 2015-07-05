using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Driver;

namespace MongoDB101
{
    // # Workaround for profiling MongoDB.Driver generated query shell syntax
    // # IAsyncCursorSourceExtensions are wrapped in order to output query to console

    #if PROFILING_ENABLED
    public static class ProfilingExtensions
    {
        #region ToListAsync

        public static async Task<List<TDocument>> ToListAsync<TDocument>(this IAsyncCursorSource<TDocument> source)
        {
            Profiler.Profile(source);

            return await IAsyncCursorSourceExtensions.ToListAsync(source);
        }

        public static async Task<List<TDocument>> ToListAsync<TDocument>(this IAsyncCursorSource<TDocument> source, CancellationToken cancellationToken)
        {
            Profiler.Profile(source);

            return await IAsyncCursorSourceExtensions.ToListAsync(source, cancellationToken);
        }

        #endregion ENDOF: ToListAsync

        #region ForEachAsync

        public static async Task ForEachAsync<TDocument>(this IAsyncCursorSource<TDocument> source, Action<TDocument> processor)
        {
            Profiler.Profile(source);

            await IAsyncCursorSourceExtensions.ForEachAsync(source, processor);
        }

        public static async Task ForEachAsync<TDocument>(this IAsyncCursorSource<TDocument> source, Action<TDocument> processor, CancellationToken cancellationToken)
        {
            Profiler.Profile(source);

            await IAsyncCursorSourceExtensions.ForEachAsync(source, processor, cancellationToken);
        }

        #endregion ENDOF: ForEachAsync
    }
    #endif
}