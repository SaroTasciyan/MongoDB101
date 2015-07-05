using System;
using System.Configuration;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

using MongoDB101.Context;

namespace MongoDB101.Tests
{
    public abstract class BaseTest
    {
        private const string MongoDbServerAddressKey = "MongoDbServerAddress";
        private const string MongoDbServerPortKey = "MongoDbServerPort";

        protected readonly TestContext testContext;
        protected readonly SchoolContext schoolContext;
        protected readonly BlogContext blogContext;
        protected readonly InventoryContext inventoryContext;

        private static string MongoDbServerAddress
        {
            get { return ConfigurationManager.AppSettings[MongoDbServerAddressKey]; }
        }

        private static string MongoDbServerPort
        {
            get { return ConfigurationManager.AppSettings[MongoDbServerPortKey]; }
        }

        protected BaseTest()
        {
            SetupCultureInfo();
            SetupMappingConventions();
            SetupProfiler();

            string connectionString = String.Format("mongodb://{0}:{1}", MongoDbServerAddress, MongoDbServerPort);
            MongoClient mongoClient = new MongoClient(connectionString);

            testContext = new TestContext(mongoClient);
            schoolContext = new SchoolContext(mongoClient);
            blogContext = new BlogContext(mongoClient);
            inventoryContext = new InventoryContext(mongoClient);

            Task testContextResetDataTask = testContext.ResetData();
            Task schoolContextResetDataTask = schoolContext.ResetData();
            Task blogContextResetDataTask = blogContext.ResetData();
            Task inventoryContextResetDataTask = inventoryContext.ResetData();

            testContextResetDataTask.Wait();
            schoolContextResetDataTask.Wait();
            blogContextResetDataTask.Wait();
            inventoryContextResetDataTask.Wait();
        }

        private static void SetupCultureInfo()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-US");
            Thread.CurrentThread.CurrentUICulture = new CultureInfo("en-US");
        }

        private static void SetupMappingConventions()
        {
            ConventionPack conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
            ConventionRegistry.Register("camelCase", conventionPack, x => true); // # For all types, apply camel case field names convention
        }

        private static void SetupProfiler()
        {
            Profiler.Formatting = (Profiler.Options.Formatting.SingleLine | Profiler.Options.Formatting.Pretty);
            Profiler.Output = Profiler.Options.Output.Conventional;
        }
    }
}