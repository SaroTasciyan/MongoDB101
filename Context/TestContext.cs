using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Models.Test;

namespace MongoDB101.Context
{
    public class TestContext : DbContext
    {
        public const string PeopleCollectionName = "people";
        public const string WidgetsCollectionName = "widgets";

        private static readonly IEnumerable<BsonDocument> widgetData;
        private static readonly IEnumerable<Person> peopleData = new Person[]
        {
            new Person { Name = "Smith", Age = 30, Profession = "Hacker" },
            new Person { Name = "George", Age = 32, Favorites = new string[] { "ice cream", "pretzels" } },
            new Person { Name = "Howard", Age = 41, Favorites = new string[] { "pretzels", "beer" } },
            new Person { Name = "Irwing", Age = 37, Favorites = new string[] { "beer", "pretzels", "cheese" } },
            new Person { Name = "John", Age = 33, Favorites = new string[] { "beer", "cheese" } },
            new Person { Name = "Jones", Age = 24, Profession = "Hacker" }
        };

        protected override string DatabaseName
        {
            get { return "test"; }
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

        public TestContext(IMongoClient mongoClient) : base(mongoClient) { }

        public override async Task ResetData()
        {
            // # Delete existing data
            await Database.DropCollectionAsync(PeopleCollectionName);
            await Database.DropCollectionAsync(WidgetsCollectionName);

            // # Create new data
            await People.InsertManyAsync(peopleData);
            await WidgetsAsBson.InsertManyAsync(widgetData);

            // # Create new indexes
            await People.Indexes.CreateOneAsync(Builders<Person>.IndexKeys.Ascending(x => x.Name).Ascending(x => x.Age));
            await Widgets.Indexes.CreateOneAsync(Builders<Widget>.IndexKeys.Ascending(x => x.X));
        }
    }
}