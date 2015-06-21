using System;
using System.Threading.Tasks;

using Xunit;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Context;
using MongoDB101.Models;

namespace MongoDB101.Tests
{
    public class UpdateTests : BaseTest
    {
        #region Replace

        [Fact]
        public async Task RepaceOneBsonDocument() // # db.<collectionName>.update(<documentFilter>, <documentReplacement>);
        {
            BsonDocument filter = new BsonDocument("_id", 5);
            BsonDocument replacement = new BsonDocument
            {
                { "_id", 5 },
                { "x", 30 }
            };
            
            ReplaceOneResult result = await testContext.WidgetsAsBson.ReplaceOneAsync(filter, replacement);

            result.IsAcknowledged.Should().BeTrue();
            result.IsModifiedCountAvailable.Should().BeTrue();
            result.MatchedCount.Should().Be(1);
            result.ModifiedCount.Should().Be(1);
            
            Console.WriteLine("Shell: db.{0}.findOne( {{ _id : {1} }} );", TestContext.WidgetsCollectionName, filter["_id"]);
        }

        [Fact]
        public void RepaceOneChangingIdFails()
        {
            BsonDocument filter = new BsonDocument("_id", 5);
            BsonDocument replacement = new BsonDocument
            {
                { "_id", 10 },
                { "x", 30 }
            };

            Func<Task> insertFunc = (async () => await testContext.WidgetsAsBson.ReplaceOneAsync(filter, replacement));
            insertFunc.ShouldThrow<MongoWriteException>("Id can not be changed!");
            
            Console.WriteLine("Shell: db.{0}.findOne( {{ _id : {1} }} );", TestContext.WidgetsCollectionName, replacement["_id"]); // # Will result null
        }

        [Fact]
        public async Task RepaceOneUpsertBsonDocument() // # db.<collectionName>.update(<documentFilter>, <documentReplacement>, { upsert : true });
        {
            BsonDocument filter = new BsonDocument("_id", 10);
            BsonDocument replacement = new BsonDocument
            {
                { "_id", 10 },
                { "x", 30 }
            };

            UpdateOptions updateOptions = new UpdateOptions { IsUpsert = true }; // # Insert if no matching documents
            ReplaceOneResult result = await testContext.WidgetsAsBson.ReplaceOneAsync(filter, replacement, updateOptions);

            result.MatchedCount.Should().Be(0);
            result.ModifiedCount.Should().Be(0);
            result.UpsertedId.Should().NotBeNull();
            result.UpsertedId.Should().Be(replacement["_id"]);

            Console.WriteLine("Shell: db.{0}.findOne( {{ _id : {1} }} );", TestContext.WidgetsCollectionName, filter["_id"]);
        }

        #endregion ENDOF: Replace

        #region Update (BsonDocument)

        [Fact]
        public async Task UpdateOneBsonDocument()
        {
            const int updatedValue = 10;
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Eq("x", 5);
            BsonDocument replacement = new BsonDocument("$inc", new BsonDocument("x", updatedValue));

            UpdateResult result = await testContext.WidgetsAsBson.UpdateOneAsync(filter, replacement);

            result.MatchedCount.Should().Be(1);
            result.ModifiedCount.Should().Be(1);

            Console.WriteLine("Shell: db.{0}.findOne( {{ x : {1} }} );", TestContext.WidgetsCollectionName, updatedValue);
        }

        [Fact]
        public async Task UpdateOneBsonDocumentWithBuilders()
        {
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Eq("x", 5);
            UpdateDefinition<BsonDocument> replacement = Builders<BsonDocument>.Update.Inc("x", 5);

            UpdateResult result = await testContext.WidgetsAsBson.UpdateOneAsync(filter, replacement);

            result.MatchedCount.Should().Be(1);
            result.ModifiedCount.Should().Be(1);

            Console.WriteLine("Shell: db.{0}.find().pretty();", TestContext.WidgetsCollectionName);
        }

        [Fact]
        public async Task UpdateOneFilterMatchingMoreThanOne()
        {
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Gt("x", 5);
            UpdateDefinition<BsonDocument> replacement = Builders<BsonDocument>.Update.Inc("x", 10);

            UpdateResult result = await testContext.WidgetsAsBson.UpdateOneAsync(filter, replacement);

            result.MatchedCount.Should().Be(1); // # Only modified one document altho filter matched more than one document
            result.ModifiedCount.Should().Be(1);

            Console.WriteLine("Shell: db.{0}.find().pretty();", TestContext.WidgetsCollectionName);
        }

        [Fact]
        public async Task UpdateManyBsonDocumentWithBuilders()
        {
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Gt("x", 5);
            UpdateDefinition<BsonDocument> replacement = Builders<BsonDocument>.Update.Inc("x", 10);

            UpdateResult result = await testContext.WidgetsAsBson.UpdateManyAsync(filter, replacement);

            result.MatchedCount.Should().BeGreaterThan(1);
            result.ModifiedCount.Should().BeGreaterThan(1);

            Console.WriteLine("Shell: db.{0}.find().pretty();", TestContext.WidgetsCollectionName);
        }

        #endregion ENDOF: Update (BsonDocument)

        #region Update (Model)

        [Fact]
        public async Task UpdateOneModelWithBuilders()
        {
            UpdateDefinition<Widget> replacement = Builders<Widget>.Update.Inc(x => x.X, 10);

            UpdateResult result = await testContext.Widgets.UpdateOneAsync(x => x.X > 5, replacement);

            result.MatchedCount.Should().Be(1);
            result.ModifiedCount.Should().Be(1);

            Console.WriteLine("Shell: db.{0}.find().pretty();", TestContext.WidgetsCollectionName);
        }

        #endregion ENDOF: Update (Model)
    }
}