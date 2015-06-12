using System.Threading.Tasks;

using Xunit;
using FluentAssertions;
using MongoDB.Driver;

using MongoDB101.Models;

namespace MongoDB101.Tests
{
    public class BulkWriteTests : BaseTest
    {
        [Fact]
        public async Task BulkWrite()
        {
            WriteModel<Widget>[] writeModelArray = new WriteModel<Widget>[]
            {
                new DeleteOneModel<Widget>(Builders<Widget>.Filter.Eq(x => x.X, 5)),
                new DeleteOneModel<Widget>(Builders<Widget>.Filter.Eq(x => x.X, 7)),
                new UpdateManyModel<Widget>(Builders<Widget>.Filter.Lt(x => x.X, 7), Builders<Widget>.Update.Inc(x => x.X, 1))
            };

            BulkWriteResult<Widget> result = await testContext.Widgets.BulkWriteAsync(writeModelArray);

            result.DeletedCount.Should().Be(2);
            result.ModifiedCount.Should().Be(6);
        }
    }
}