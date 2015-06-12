using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Models;

namespace MongoDB101.Context
{
    public class TestContext
    {
        public const string DatabaseName = "test";
        public const string PeopleCollectionName = "people";
        public const string WidgetsCollectionName = "widgets";

        private static readonly IEnumerable<BsonDocument> widgetData;
        private static readonly IEnumerable<Person> peopleData = new Person[]
        {
            new Person { Name = "Smith", Age = 30, Profession = "Hacker" },
            new Person { Name = "Jones", Age = 24, Profession = "Hacker" }
        };

        private readonly IMongoClient mongoClient;
        private IMongoDatabase database;

        private IMongoDatabase Database
        {
            get { return database ?? (database = mongoClient.GetDatabase(DatabaseName)); }
        }

        public IMongoCollection<BsonDocument> PeopleAsBson
        {
            get { return Database.GetCollection<BsonDocument>(PeopleCollectionName); }
        }

        public IMongoCollection<Person> People
        {
            get { return Database.GetCollection<Person>(PeopleCollectionName); }
        }

        public IMongoCollection<BsonDocument> WidgetsAsBson
        {
            get { return Database.GetCollection<BsonDocument>(WidgetsCollectionName); }
        }

        public IMongoCollection<Widget> Widgets
        {
            get { return Database.GetCollection<Widget>(WidgetsCollectionName); }
        }

        static TestContext()
        {
            widgetData = Enumerable
                .Range(0, 10)
                .Select(x => new BsonDocument("_id", x).Add("x", x));
        }

        public TestContext(IMongoClient mongoClient)
        {
            this.mongoClient = mongoClient;
        }

        public async Task ResetData()
        {
            // # Delete existing data
            await Database.DropCollectionAsync(PeopleCollectionName);
            await Database.DropCollectionAsync(WidgetsCollectionName);

            // # Create new data
            await People.InsertManyAsync(peopleData);
            await WidgetsAsBson.InsertManyAsync(widgetData);
        }
    }
}