using System;
using System.Threading.Tasks;

using Xunit;
using FluentAssertions;
using MongoDB.Driver;

using MongoDB101.Context;

namespace MongoDB101.Tests
{
    public class DeleteTests : BaseTest
    {
        [Fact]
        public async Task DeleteOneWithExpression()
        {
            DeleteResult result = await testContext.Widgets.DeleteOneAsync(x => x.X > 5);
            
            result.IsAcknowledged.Should().BeTrue();
            result.DeletedCount.Should().Be(1); // # Deleted one element altho filter matches more than one element

            Console.WriteLine("Shell: db.{0}.findOne( {{ x : {1} }} );", TestContext.WidgetsCollectionName, 6); // # Will result null
        }

        [Fact]
        public async Task DeleteManyWithExpression()
        {
            DeleteResult result = await testContext.Widgets.DeleteManyAsync(x => x.X > 5);

            result.IsAcknowledged.Should().BeTrue();
            result.DeletedCount.Should().BeGreaterThan(1);

            Console.WriteLine("Shell: db.{0}.findOne( {{ x : {1} }} );", TestContext.WidgetsCollectionName, 6); // # Will result null
        }
    }
}