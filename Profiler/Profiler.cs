using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

using Humanizer;
using Newtonsoft.Json.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

using JSONFormatting = Newtonsoft.Json.Formatting;

// ReSharper disable MemberHidesStaticFromOuterClass
namespace MongoDB101
{
    public static class Profiler
    {
        #region Definitions

        private const string UnresolvedCollectionOutput = "<collection>";
        private const string OutputHeader = "*** Generated Query ****";
        private const string OutputHeaderPretty = "*** Generated Query (Pretty) ****";

        public static class Options
        {
            public enum Output
            {
                None,
                Original, //TODO
                Conventional,
                FunctionOnly
            }

            [Flags]
            public enum Formatting
            {
                SingleLine = 0x0,
                Pretty = 0x1
            }
        }

        #endregion ENDOF: Definitions

        #region Properties

        public static Options.Output Output { get; set; }
        public static Options.Formatting Formatting { get; set; }

        #endregion ENDOF: Properties

        #region Methods

        public static bool Profile<TDocument>(IAsyncCursorSource<TDocument> source)
        {
            if (Output == Options.Output.None) { return false; }

            bool anyOutput = false;
            try
            {
                string functionOutput = source.ToString();
                string dbAndCollectionOutput = (Output != Options.Output.FunctionOnly ? String.Format("db.{0}", GetCollectionName(source)) : String.Empty);

                if (Formatting.HasFlag(Options.Formatting.SingleLine))
                {
                    Console.WriteLine(OutputHeader);
                    Console.WriteLine("{0}.{1}", dbAndCollectionOutput, functionOutput);

                    anyOutput = true;
                }

                if (Formatting.HasFlag(Options.Formatting.Pretty))
                {
                    string indentedFunctionOutput = GetIndentedQueryShellSyntax(source);

                    if (functionOutput != indentedFunctionOutput)
                    {
                        if (anyOutput) { Console.WriteLine(); } // # Extra space for second query output (pretty)

                        Console.WriteLine(OutputHeaderPretty);
                        Console.WriteLine("{0}.{1}", dbAndCollectionOutput, indentedFunctionOutput);

                        anyOutput = true;
                    }
                }
            }
            catch { anyOutput = false; }

            return anyOutput;
        }

        #endregion ENDOF: Methods

        #region Helpers

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

        private static string GetCollectionName<TDocument>(IAsyncCursorSource<TDocument> source)
        {
            switch (Output)
            {
                case Options.Output.None: return String.Empty;
                case Options.Output.Original: return GetOriginalCollectionName(source);
                case Options.Output.Conventional: return GetConventionalCollectionName(source);
                case Options.Output.FunctionOnly: return String.Empty;
                default: throw new Exception("Undefined Profiler.Options.Output value");
            }
        }

        private static string GetConventionalCollectionName<TDocument>(IAsyncCursorSource<TDocument> source)
        {
            if (source is IAsyncCursorSource<BsonDocument> || IsAnonymous<TDocument>()) { return UnresolvedCollectionOutput; }

            string modelName = typeof(TDocument).Name;

            IConventionPack conventionPack = ConventionRegistry.Lookup(typeof(CamelCaseElementNameConvention));
            bool isCamelCaseConvention = conventionPack.Conventions.Any();

            string collectionName = isCamelCaseConvention ? modelName.ToLower() : modelName;

            return collectionName.Pluralize(false);
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
                indentedDocument = JToken.Parse(indentedDocument).ToString(JSONFormatting.Indented);
            }

            return String.Format("{0}({1})", function, indentedDocument);
        }

        [Obsolete("Incomplete implmenetation", false)]
        private static string GetOriginalCollectionName<TDocument>(IAsyncCursorSource<TDocument> source)
        {
            const BindingFlags flags = (BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

            Assembly assembly = typeof(IFindFluent<,>).Assembly;
            Type findFluentType = assembly.GetType("MongoDB.Driver.FindFluent`2").MakeGenericType(typeof(TDocument), typeof(TDocument)); //TODO: The type of second generic type argument is required to be known!
            FieldInfo collectionFieldInfo = findFluentType.GetFields(flags).SingleOrDefault(x => x.Name == "_collection");

            string collectionName = null;
            if (collectionFieldInfo != null)
            {
                IMongoCollection<TDocument> collection = collectionFieldInfo.GetValue(source) as IMongoCollection<TDocument>;

                collectionName = (collection == null ? null : collection.CollectionNamespace.CollectionName);
            }

            //return collectionName;
            throw new NotImplementedException();
        }

        #endregion ENDOF: Helpers
    }
}