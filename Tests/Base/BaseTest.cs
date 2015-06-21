using System;
using System.Configuration;

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
            SetupMappingConventions();

            string connectionString = String.Format("mongodb://{0}:{1}", MongoDbServerAddress, MongoDbServerPort);
            MongoClient mongoClient = new MongoClient(connectionString);

            testContext = new TestContext(mongoClient);
            schoolContext = new SchoolContext(mongoClient);

            testContext.ResetData().Wait();
            schoolContext.ResetData().Wait();
        }

        private static void SetupMappingConventions()
        {
            ConventionPack conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
            ConventionRegistry.Register("camelCase", conventionPack, x => true); // # For all types, apply camel case field names convention
        }
    }
}