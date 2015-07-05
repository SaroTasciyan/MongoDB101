using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Humanizer;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MongoDB101
{
    // # Workaround for profiling MongoDB.Driver generated query shell syntax
    // # IAsyncCursorSourceExtensions are wrapped in order to output query to console

    #if DEBUG
    public static class ProfilingExtensions
    {
        #region ToListAsync

        public static async Task<List<TDocument>> ToListAsync<TDocument>(this IAsyncCursorSource<TDocument> source)
        {
            source.Profile();

            return await IAsyncCursorSourceExtensions.ToListAsync(source);
        }

        public static async Task<List<TDocument>> ToListAsync<TDocument>(this IAsyncCursorSource<TDocument> source, CancellationToken cancellationToken)
        {
            source.Profile();

            return await IAsyncCursorSourceExtensions.ToListAsync(source, cancellationToken);
        }

        #endregion ENDOF: ToListAsync

        #region ForEachAsync

        public static async Task ForEachAsync<TDocument>(this IAsyncCursorSource<TDocument> source, Action<TDocument> processor)
        {
            source.Profile();

            await IAsyncCursorSourceExtensions.ForEachAsync(source, processor);
        }

        public static async Task ForEachAsync<TDocument>(this IAsyncCursorSource<TDocument> source, Action<TDocument> processor, CancellationToken cancellationToken)
        {
            source.Profile();

            await IAsyncCursorSourceExtensions.ForEachAsync(source, processor, cancellationToken);
        }

        #endregion ENDOF: ForEachAsync

        #region Helpers

        private static void Profile<TDocument>(this IAsyncCursorSource<TDocument> source)
        {
            try
            {
                Type type = typeof(TDocument);

                bool isAnonymousOrBsonDocument = (source is IAsyncCursorSource<BsonDocument> || IsAnonymous<TDocument>());
                string collection = isAnonymousOrBsonDocument ? "<collection>" : GetCollectionName(type.Name);

                Console.WriteLine("*** Generated Query *** \ndb.{0}.{1};\n", collection, source);

                string indentedQueryShellSytax = GetIndentedQueryShellSyntax(source);

                if (source.ToString() != indentedQueryShellSytax)
                {
                    Console.WriteLine("*** Generated Query (Pretty) *** \ndb.{0}.{1};", collection, indentedQueryShellSytax);
                }
            }
            catch { /* # Ignored */ }
        }

        private static bool IsAnonymous<T>()
        {
            return typeof(T).IsAnonymous();
        }

        // #Ref: http://stackoverflow.com/questions/2483023/how-to-test-if-a-type-is-anonymous
        private static bool IsAnonymous(this Type type)
        {
            if (type == null) { throw new ArgumentNullException("type", "Argument can not be null"); }

            return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                && type.IsGenericType && type.Name.Contains("AnonymousType")
                && (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
                && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }

        private static string GetCollectionName(string modelName)
        {
            return modelName.ToLower().Pluralize(false);
        }

        private static string GetIndentedQueryShellSyntax<TDocument>(IAsyncCursorSource<TDocument> source)
        {
            string query = source.ToString();

            Regex queryRegex = new Regex(@"^(?<Function>[a-zA-Z]+)\((?<Document>.*)\)$");
            Match queryMatch = queryRegex.Match(query);

            if (queryMatch.Groups.Count == 0) { throw new ArgumentException("Invalid query", "source"); }

            string function = queryMatch.Groups["Function"].Value;
            string indentedDocument = queryMatch.Groups["Document"].Value;

            if (indentedDocument != "{ }")
            {
                indentedDocument = JToken.Parse(indentedDocument).ToString(Formatting.Indented);
            }

            return String.Format("{0}({1})", function, indentedDocument);
        }

        //TODO # Implement ResolveCollectionName
        [Obsolete("Incomplete", true)]
        private static void ResolveCollectionName<TDocument>(this IAsyncCursorSource<TDocument> source)
        {
            Assembly assembly = typeof(IFindFluent<,>).Assembly;
            Type findFluentType = assembly.GetType("MongoDB.Driver.FindFluent`2").MakeGenericType(typeof(TDocument), typeof(object));

            FieldInfo collectionFieldInfo = findFluentType.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance).SingleOrDefault(x => x.Name == "_collection");

            if (collectionFieldInfo != null)
            {
                IMongoCollection<TDocument> collection = collectionFieldInfo.GetValue(source) as IMongoCollection<TDocument>;
            }
        }

        #endregion ENDOF: Helpers
    }
    #endif
}