using System.Collections.Generic;
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

        public TestContext(IMongoClient mongoClient)
        {
            this.mongoClient = mongoClient;
        }

        public async Task ResetData()
        {
            // # Delete existing data
            await Database.DropCollectionAsync(PeopleCollectionName);

            // # Create new data
            await People.InsertManyAsync(peopleData);
        }
    }
}