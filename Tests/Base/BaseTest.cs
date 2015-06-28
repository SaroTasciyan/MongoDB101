﻿using System;
using System.Configuration;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

using MongoDB101.Context;

namespace MongoDB101.Tests
{
    //TODO: Add profiling option for MongoDb.Driver generated query shell syntax
    //TODO: Add samples from;
    // http://docs.mongodb.org/manual/reference/sql-comparison/
    // http://docs.mongodb.org/manual/reference/sql-aggregation-comparison/
    
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
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-US");
            Thread.CurrentThread.CurrentUICulture = new CultureInfo("en-US");

            SetupMappingConventions();

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

        private static void SetupMappingConventions()
        {
            ConventionPack conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
            ConventionRegistry.Register("camelCase", conventionPack, x => true); // # For all types, apply camel case field names convention
        }
    }
}