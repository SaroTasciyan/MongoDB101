using System.Threading.Tasks;

using Xunit;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Models.Test;

// ReSharper disable InconsistentNaming
namespace MongoDB101.Tests
{
    public class CountTests : BaseTest
    {
        [Fact]
        public async Task CountAll()
        {
            long count = await testContext.PeopleAsBson.CountAsync(new BsonDocument());

            count.Should().Be(6);
        }

        [Fact]
        public async Task CountFilteredWithBsonDocument()
        {
            BsonDocument filter = new BsonDocument("profession", "Hacker");
            long hackersCount = await testContext.PeopleAsBson.CountAsync(filter);

            hackersCount.Should().Be(2);
        }

        [Fact]
        public async Task CountFilteredWithJSONString()
        {
            long hackersCount = await testContext.PeopleAsBson.CountAsync("{ profession : 'Hacker' }");

            hackersCount.Should().Be(2);
        }

        [Fact]
        public async Task CountFilteredWithBuilderOfModel()
        {
            FilterDefinitionBuilder<Person> builder = Builders<Person>.Filter;
            FilterDefinition<Person> filter = builder.Eq(x => x.Profession, "Hacker");

            long hackersCount = await testContext.People.CountAsync(filter);

            hackersCount.Should().Be(2);
        }

        [Fact]
        public async Task CountFilteredWithExpressionTree()
        {
            long hackersCount = await testContext.People.CountAsync(x => x.Profession == "Hacker");

            hackersCount.Should().Be(2);
        }
    }
}