using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Xunit;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Context;
using MongoDB101.Models;

namespace MongoDB101.Tests
{
    public class InsertTests : BaseTest
    {
        #region Insert (BsonDocument)

        [Fact]
        public void InsertOneBsonDocument() // # db.<collectionName>.insert(<document>);
        {
            BsonDocument bsonDocument = new BsonDocument
            {
                { "name", "Smith" },
                { "age", 30 },
                { "profession", "Hacker" }
            };

            bsonDocument.Contains("_id").Should().BeFalse();

            Func<Task> insertFunc = (async () => await testContext.PeopleAsBson.InsertOneAsync(bsonDocument));
            insertFunc.ShouldNotThrow();

            bsonDocument.Contains("_id").Should().BeTrue();
            bsonDocument["_id"].Should().NotBe(ObjectId.Empty);

            Console.WriteLine("Shell: db.{0}.find( {{ _id : ObjectId('{1}') }} ).pretty();", TestContext.PeopleCollectionName, bsonDocument["_id"]);
        }

        [Fact]
        public void InsertManyBsonDocument() // # db.<collectionName>.insert([<document1>, ... , <documentN>]);
        {
            BsonDocument bsonDocument1 = new BsonDocument
            {
                { "name", "Smith" },
                { "age", 30 },
                { "profession", "Hacker" }
            };

            BsonDocument bsonDocument2 = new BsonDocument
            {
                { "name", "Jones" },
                { "age", 24 },
                { "profession", "Hacker" }
            };

            bsonDocument1.Contains("_id").Should().BeFalse(); // # Documents do not contain Id before insert
            bsonDocument2.Contains("_id").Should().BeFalse();

            Func<Task> insertFunc = (async () => await testContext.PeopleAsBson.InsertManyAsync(new[] { bsonDocument1, bsonDocument2 }));
            insertFunc.ShouldNotThrow();

            bsonDocument1.Contains("_id").Should().BeTrue(); // # Non empty ObjectId value is generated for document is assigned after insert
            bsonDocument2.Contains("_id").Should().BeTrue();
            bsonDocument1["_id"].Should().NotBe(ObjectId.Empty);
            bsonDocument2["_id"].Should().NotBe(ObjectId.Empty);

            Console.WriteLine("Shell: db.{0}.find( {{ _id : {{ $in : [ObjectId('{1}'), ObjectId('{2}')] }} }} ).pretty();", TestContext.PeopleCollectionName, bsonDocument1["_id"], bsonDocument2["_id"]);
        }

        #endregion ENDOF: Insert (BsonDocument)

        #region Insert (Model)

        [Fact]
        public void InsertOneModel()
        {
            Person person = new Person
            {
                Name = "Smith",
                Age = 30,
                Profession = "Hacker"
            };

            person.Id.Should().Be(ObjectId.Empty);

            Func<Task> insertFunc = (async () => await testContext.People.InsertOneAsync(person));
            insertFunc.ShouldNotThrow();

            person.Id.Should().NotBe(ObjectId.Empty);

            Console.WriteLine("Shell: db.{0}.find( {{ _id : ObjectId('{1}') }} ).pretty();", TestContext.PeopleCollectionName, person.Id);
        }

        [Fact]
        public void InsertManyModel()
        {
            List<Person> personList = new List<Person>
            {
                new Person { Name = "Smith", Age = 30, Profession = "Hacker" },
                new Person { Name = "Jones", Age = 25, Profession = "Hacker" }
            };

            Func<Task> insertFunc = (async () => await testContext.People.InsertManyAsync(personList));
            insertFunc.ShouldNotThrow();
            
            personList.ForEach(x => x.Should().NotBe(ObjectId.Empty));

            Console.WriteLine("Shell: db.{0}.find( {{ _id : {{ $in : [ObjectId('{1}'), ObjectId('{2}')] }} }} ).pretty();", TestContext.PeopleCollectionName, personList[0].Id, personList[1].Id);
        }

        [Fact]
        public void InsertModelDuplicateKeyException()
        {
            Person person = new Person
            {
                Name = "Smith",
                Age = 30,
                Profession = "Hacker"
            };

            Func<Task> insertFunc = (async () => await testContext.People.InsertOneAsync(person));
            insertFunc.ShouldNotThrow();

            insertFunc.ShouldThrow<MongoWriteException>().WithInnerException<MongoBulkWriteException>();
        }

        #endregion ENDOF: Insert (Model)
    }
}