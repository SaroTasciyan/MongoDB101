using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Xunit;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using MongoDB101.Models;

// ReSharper disable InconsistentNaming
namespace MongoDB101.Tests
{
    //TODO # Dot Notation Tests
    public class FindTests : BaseTest
    {
        #region Find (BsonDocument)

        [Fact]
        public async Task FindBsonDocumentWithCursor() // # db.<collectionName>.find()
        {
            List<BsonDocument> bsonDocumentList = new List<BsonDocument>();

            BsonDocument filter = new BsonDocument(); // # Applying empty filter
            using (IAsyncCursor<BsonDocument> cursor = await testContext.PeopleAsBson.Find(filter).ToCursorAsync())
            {
                while (await cursor.MoveNextAsync()) // # Retrieves one batch per iteration (with document number of batch size)
                {
                    foreach (BsonDocument document in cursor.Current)
                    {
                        bsonDocumentList.Add(document);
                    }
                }
            }

            bsonDocumentList.Should().HaveCount(6);
            bsonDocumentList.First()["name"].Should().Be("Smith"); // # Boundary check
            bsonDocumentList.Last()["name"].Should().Be("Jones");
        }

        [Fact]
        public async Task FindBsonDocumentWithForEach()
        {
            List<BsonDocument> bsonDocumentList = new List<BsonDocument>();

            BsonDocument filter = new BsonDocument();
            await testContext.PeopleAsBson.Find(filter).ForEachAsync(x => bsonDocumentList.Add(x));

            bsonDocumentList.Should().HaveCount(6);
            bsonDocumentList.First()["name"].Should().Be("Smith");
            bsonDocumentList.Last()["name"].Should().Be("Jones");
        }

        [Fact]
        public async Task FindBsonDocumentWithToList()
        {
            BsonDocument filter = new BsonDocument();
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(6);
            bsonDocumentList.First()["name"].Should().Be("Smith");
            bsonDocumentList.Last()["name"].Should().Be("Jones");
        }

        #endregion ENDOF: Find (BsonDocument)

        #region Find (Model)

        [Fact]
        public async Task FindModelWithToList()
        {
            BsonDocument filter = new BsonDocument();
            List<Person> personList = await testContext.People.Find(filter).ToListAsync();

            personList.Should().HaveCount(6);
            personList.First().Name.Should().Be("Smith");
            personList.Last().Name.Should().Be("Jones");
        }

        [Fact]
        public async Task FindModelWithCursor()
        {
            List<Person> personList = new List<Person>();

            BsonDocument filter = new BsonDocument();
            using (IAsyncCursor<Person> cursor = await testContext.People.Find(filter).ToCursorAsync())
            {
                while (await cursor.MoveNextAsync())
                {
                    foreach (Person person in cursor.Current)
                    {
                        personList.Add(person);
                    }
                }
            }

            personList.Should().HaveCount(6);
            personList.First().Name.Should().Be("Smith");
            personList.Last().Name.Should().Be("Jones");
        }

        [Fact]
        public async Task FindModelWithForEach()
        {
            List<Person> personList = new List<Person>();

            BsonDocument filter = new BsonDocument();
            await testContext.People.Find(filter).ForEachAsync(x => personList.Add(x));

            personList.Should().HaveCount(6);
            personList.First().Name.Should().Be("Smith");
            personList.Last().Name.Should().Be("Jones");
        }

        #endregion ENDOF: Find (Model)

        #region Find Filtered (BsonDocument)

        [Fact]
        public async Task FindFilteredWithBsonDocument() // # db.<collectionName>.find(<document>)
        {
            BsonDocument filter = new BsonDocument("name", "Smith");
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();
            
            bsonDocumentList.Should().HaveCount(1);
            bsonDocumentList.First()["name"].Should().Be("Smith");
        }

        [Fact]
        public async Task FindFilteredWithJSONString() 
        {
            const string filter = "{ name: 'Smith' }";
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(1);
            bsonDocumentList.First()["name"].Should().Be("Smith");
        }

        [Fact]
        public async Task FindFilteredArrayWithSJONString()
        {
            const string filter = "{ tags: 'LINQ' }";
            List<BsonDocument> bsonDocumentList = await blogContext.PostsAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
            bsonDocumentList.First()["title"].Should().Be("Query Expression Syntax");
            bsonDocumentList.Last()["title"].Should().Be("Computing a Cartesian Product");
        }

        [Fact]
        public async Task FindFilteredWithNestedBsonDocument()
        {
            BsonDocument filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument("age", new BsonDocument("$lt", 30)),
                new BsonDocument("name", "Smith")
            }); // # (age < 30 && name == "Smith")

            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().BeEmpty();
        }

        [Fact]
        public async Task FindFilteredWithBuilder()
        {
            FilterDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = builder.And(builder.Lt("age", 30), builder.Eq("name", "Smith"));

            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().BeEmpty();
        }

        [Fact]
        public async Task FindFilteredWithBuilderOperator()
        {
            FilterDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = builder.Lt("age", 30) & !builder.Eq("name", "Jones");

            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().BeEmpty();
        }

        [Fact]
        public async Task FindFilteredExistsWithJSONString()
        {
            const string filter = "{ profession: { $exists : true } }"; // # Profession field exists
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(6);
            bsonDocumentList.First()["name"].Should().Be("Smith");
            bsonDocumentList.Last()["name"].Should().Be("Jones");
        }

        [Fact]
        public async Task FindFilteredExistsWithBuilder()
        {
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Exists("profession");
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(6);
            bsonDocumentList.First()["name"].Should().Be("Smith");
            bsonDocumentList.Last()["name"].Should().Be("Jones");
        }

        [Fact]
        public async Task FindFilteredRegexWithJSONString()
        {
            const string filter = "{ name: { $regex : '^[A-Z][a-z]{4}$' } }";
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
        }

        [Fact]
        public async Task FindFilteredRegexWithBuilder()
        {
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Regex("name", new BsonRegularExpression("^[A-Z][a-z]{4}$"));
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
        }

        [Fact]
        public async Task FindFilteredArrayAllWithJSONString()
        {
            const string filter = "{ favorites: { $all : [ 'pretzels', 'beer' ] } }";
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
            bsonDocumentList.First()["name"].Should().Be("Howard");
            bsonDocumentList.Last()["name"].Should().Be("Irwing");
        }

        [Fact]
        public async Task FindFilteredArrayAllWithBuilder()
        {
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.All("favorites", new string[] { "pretzels", "beer" });
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
            bsonDocumentList.First()["name"].Should().Be("Howard");
            bsonDocumentList.Last()["name"].Should().Be("Irwing");
        }

        [Fact]
        public async Task FindFilteredArrayInWithJSONString()
        {
            const string filter = "{ name: { $in : [ 'Howard', 'John' ] } }";
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
            bsonDocumentList.First()["name"].Should().Be("Howard");
            bsonDocumentList.Last()["name"].Should().Be("John");
        }

        [Fact]
        public async Task FindFilteredArrayInWithBuilder()
        {
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.In("name", new string[] { "Howard", "John" });
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
            bsonDocumentList.First()["name"].Should().Be("Howard");
            bsonDocumentList.Last()["name"].Should().Be("John");
        }

        #endregion Find Filtered (BsonDocument)

        #region Find Filtered (Model)

        [Fact]
        public async Task FindFilteredWithBuilderOfModel()
        {
            FilterDefinitionBuilder<Person> builder = Builders<Person>.Filter;
            FilterDefinition<Person> filter = builder.Lt(x => x.Age, 30) & builder.Eq(x => x.Name, "Jones");

            List<Person> personList = await testContext.People.Find(filter).ToListAsync();

            personList.Should().HaveCount(1);
            personList.First().Name.Should().Be("Jones");
        }

        [Fact]
        public async Task FindFilteredWithExpressionTree()
        {
            List<Person> personList = await testContext.People
                .Find(x => x.Age < 30 && x.Name == "Jones")
                .ToListAsync();

            personList.Should().HaveCount(1);
            personList.First().Name.Should().Be("Jones");
        }

        [Fact]
        public async Task FindFilteredArrayWithModel()
        {
            List<Post> postList = await blogContext.Posts
               .Find(x => x.Tags.Contains("LINQ"))
               .ToListAsync();

            postList.Should().HaveCount(2);
            postList.First().Title.Should().Be("Query Expression Syntax");
            postList.Last().Title.Should().Be("Computing a Cartesian Product");
        }

        [Fact]
        public async Task FindFilteredExistsWithBuilderOfModel()
        {
            FilterDefinition<Person> filter = Builders<Person>.Filter.Exists(x => x.Profession);
            List<Person> bsonDocumentList = await testContext.People.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(6);
            bsonDocumentList.First().Name.Should().Be("Smith");
            bsonDocumentList.Last().Name.Should().Be("Jones");
        }

        [Fact]
        public async Task FindFilteredRegexWithBuilderOfModel()
        {
            FilterDefinition<Person> filter = Builders<Person>.Filter.Regex(x => x.Name, new BsonRegularExpression("^[A-Z][a-z]{4}$"));
            List<Person> bsonDocumentList = await testContext.People.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
        }

        [Fact]
        public async Task FindFilteredArrayAllWithBuilderOfModel()
        {
            FilterDefinition<Person> filter = Builders<Person>.Filter.All(x => x.Favorites, new string[] { "pretzels", "beer" });
            List<Person> bsonDocumentList = await testContext.People.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
            bsonDocumentList.First().Name.Should().Be("Howard");
            bsonDocumentList.Last().Name.Should().Be("Irwing");
        }

        [Fact]
        public async Task FindFilteredArrayInWithBuilderOfModel()
        {
            FilterDefinition<Person> filter = Builders<Person>.Filter.In(x => x.Name, new string[] { "Howard", "John" });
            List<Person> bsonDocumentList = await testContext.People.Find(filter).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
            bsonDocumentList.First().Name.Should().Be("Howard");
            bsonDocumentList.Last().Name.Should().Be("John");
        }

        [Fact]
        public async Task FindFilteredArrayInWithExpressionTree()
        {
            string[] names = new string[] { "Howard", "John" };
            List<Person> bsonDocumentList = await testContext.People.Find(x => names.Contains(x.Name)).ToListAsync();

            bsonDocumentList.Should().HaveCount(2);
            bsonDocumentList.First().Name.Should().Be("Howard");
            bsonDocumentList.Last().Name.Should().Be("John");
        }

        #endregion ENDOF: Find Filtered (Model)

        #region Find Sorted (BsonDocument)

        [Fact]
        public async Task FindSortedWithBsonDocument() // # db.<collectionName>.find().sort(<document>)
        {
            BsonDocument filter = new BsonDocument();
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson
                .Find(filter)
                .Sort(new BsonDocument("age", 1))
                .ToListAsync();

            bsonDocumentList.Should().HaveCount(6);
            bsonDocumentList.First()["name"].Should().Be("Jones");
            bsonDocumentList.Last()["name"].Should().Be("Howard");
        }

        [Fact]
        public async Task FindSortedWithJSONString()
        {
            BsonDocument filter = new BsonDocument();
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson
                .Find(filter)
                .Sort("{ age : 1 }") // # Sort by age ascending
                .ToListAsync();

            bsonDocumentList.Should().HaveCount(6);
            bsonDocumentList.First()["name"].Should().Be("Jones");
            bsonDocumentList.Last()["name"].Should().Be("Howard");
        }

        [Fact]
        public async Task FindSortedWithBuilder()
        {
            BsonDocument filter = new BsonDocument();
            List<BsonDocument> bsonDocumentList = await testContext.PeopleAsBson
                .Find(filter)
                .Sort(Builders<BsonDocument>.Sort.Ascending("age").Descending("name"))
                .ToListAsync();

            bsonDocumentList.Should().HaveCount(6);
            bsonDocumentList.First()["name"].Should().Be("Jones");
            bsonDocumentList.Last()["name"].Should().Be("Howard");
        }

        #endregion ENDOF: Find Sorted (BsonDocument)

        #region Find Sorted (Model)

        [Fact]
        public async Task FindSortedWithBuilderOfModel()
        {
            BsonDocument filter = new BsonDocument();
            List<Person> personList = await testContext.People
                .Find(filter)
                .Sort(Builders<Person>.Sort.Ascending(x => x.Age).Descending(x => x.Name))
                .ToListAsync();

            personList.Should().HaveCount(6);
            personList.First().Name.Should().Be("Jones");
            personList.Last().Name.Should().Be("Howard");
        }

        [Fact]
        public async Task FindSortedWithExpressionTree()
        {
            BsonDocument filter = new BsonDocument();
            List<Person> personList = await testContext.People
                .Find(filter)
                .SortBy(x => x.Age)
                .ThenByDescending(x => x.Name)
                .ToListAsync();

            personList.Should().HaveCount(6);
            personList.First().Name.Should().Be("Jones");
            personList.Last().Name.Should().Be("Howard");
        }

        #endregion ENDOF: Find Sorted (Model)

        #region Find with Limit and Skip

        [Fact]
        public async Task FindWithLimit()
        {
            BsonDocument filter = new BsonDocument();
            List<Person> personList = await testContext.People
                .Find(filter)
                .Limit(1)
                .ToListAsync();

            personList.Should().HaveCount(1);
        }

        [Fact]
        public async Task FindWithSkip()
        {
            BsonDocument filter = new BsonDocument();
            List<Person> personList = await testContext.People
                .Find(filter)
                .Skip(1)
                .ToListAsync();

            personList.Should().HaveCount(5);
        }

        #endregion ENDOF: Find with Limit and Skip

        #region Find Projection (BsonDocument)

        [Fact]
        public async Task FindProjectionWithBsonDocument()
        {
            BsonDocument filter = new BsonDocument();
            List<BsonDocument> personList = await testContext.People
                .Find(filter)
                .Project(new BsonDocument("name", true).Add("_id", false))
                .ToListAsync();

            personList.Should().HaveCount(6);

            personList.All(x => x.Contains("name")).Should().BeTrue();
            personList.Any(x => x.Contains("_id")).Should().BeFalse();

            personList.First()["name"].Should().Be("Smith");
            personList.Last()["name"].Should().Be("Jones");
        }

        [Fact]
        public async Task FindProjectionWithJSONString()
        {
            BsonDocument filter = new BsonDocument();
            List<BsonDocument> personList = await testContext.People
                .Find(filter)
                .Project("{ name : true, _id : false }")
                .ToListAsync();

            personList.Should().HaveCount(6);

            personList.All(x => x.Contains("name")).Should().BeTrue();
            personList.Any(x => x.Contains("_id")).Should().BeFalse();

            personList.First()["name"].Should().Be("Smith");
            personList.Last()["name"].Should().Be("Jones");
        }

        [Fact]
        public async Task FindProjectionWithBuilder()
        {
            BsonDocument filter = new BsonDocument();
            List<BsonDocument> personList = await testContext.PeopleAsBson
                .Find(filter)
                .Project(Builders<BsonDocument>.Projection.Include("name").Exclude("_id"))
                .ToListAsync();

            personList.Should().HaveCount(6);

            personList.All(x => x.Contains("name")).Should().BeTrue();
            personList.Any(x => x.Contains("_id")).Should().BeFalse();

            personList.First()["name"].Should().Be("Smith");
            personList.Last()["name"].Should().Be("Jones");
        }

        #endregion ENDOF: Find With Projection (BsonDocument)

        #region Find Projection (Anonymous Type)

        [Fact]
        public async Task FindProjectionWithExpressionTree()
        {
            BsonDocument filter = new BsonDocument();
            var varList = await testContext.People
                .Find(filter)
                .Project(x => new { x.Name, CalculatedAge = x.Age + 20 }) // # Age calculation is run on client side (memory of application, not mongodb server)
                .ToListAsync();

            varList.Should().HaveCount(6);

            varList.First().CalculatedAge.Should().Be(50);
            varList.Last().CalculatedAge.Should().Be(44);
        }

        #endregion ENDOF: Find Projection (Anonymous Type)
    }
}