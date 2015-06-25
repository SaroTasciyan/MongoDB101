using System;
using System.Threading.Tasks;

using Xunit;
using FluentAssertions;
using MongoDB.Driver;

using MongoDB101.Context;
using MongoDB101.Models.Test;

namespace MongoDB101.Tests
{
    public class FindOneTests : BaseTest // # FindOneAndUpdate, FindOneAndReplace, FindOneAndDelete are all 'atomic' operations
    {
        [Fact]
        public async Task FindOneAndUpdate()
        {
            UpdateDefinition<Widget> replacement = Builders<Widget>.Update.Inc(x => x.X, 1);

            Widget result = await testContext.Widgets.FindOneAndUpdateAsync(x => x.X > 5, replacement);

            result.X.Should().Be(6); // # X value of updated Widget instance, before update.

            Console.WriteLine("Shell: db.{0}.findOne( {{ x : {1} }} );", TestContext.WidgetsCollectionName, 6); // # Will result null
        }

        [Fact]
        public async Task FindOneAndUpdateReturnDocumentAfter()
        {
            UpdateDefinition<Widget> replacement = Builders<Widget>.Update.Inc(x => x.X, 1);
            FindOneAndUpdateOptions<Widget, Widget> findOneAndUpdateOptions = new FindOneAndUpdateOptions<Widget, Widget> { ReturnDocument = ReturnDocument.After };

            Widget result = await testContext.Widgets.FindOneAndUpdateAsync<Widget>(x => x.X > 5, replacement, findOneAndUpdateOptions);

            result.X.Should().Be(7); // # X value of updated Widget instance, after update.

            Console.WriteLine("Shell: db.{0}.findOne( {{ x : {1} }} );", TestContext.WidgetsCollectionName, 6); // # Will result null
        }

        [Fact]
        public async Task FindOneAndReplaceReturnDocumentAfter()
        {
            Widget widget = new Widget { Id = 6, X = 7 };
            FindOneAndReplaceOptions<Widget, Widget> findOneAndReplaceOptions = new FindOneAndReplaceOptions<Widget, Widget> { ReturnDocument = ReturnDocument.After };

            Widget result = await testContext.Widgets.FindOneAndReplaceAsync<Widget>(x => x.X > 5, widget, findOneAndReplaceOptions);

            result.X.Should().Be(widget.X);

            Console.WriteLine("Shell: db.{0}.findOne( {{ x : {1} }} );", TestContext.WidgetsCollectionName, 6); // # Will result null
        }

        [Fact]
        public async Task FindOneAndDelete()
        {
            FindOneAndDeleteOptions<Widget, Widget> findOneAndReplaceOptions = new FindOneAndDeleteOptions<Widget, Widget>
            {
                Sort = Builders<Widget>.Sort.Descending(x => x.X)
            };

            await testContext.Widgets.FindOneAndDeleteAsync<Widget>(x => x.X > 5, findOneAndReplaceOptions);

            Console.WriteLine("Shell: db.{0}.findOne( {{ x : {1} }} );", TestContext.WidgetsCollectionName, 9); // # Will result null
        }
    }
}