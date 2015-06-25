using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Xunit;
using FluentAssertions;

using MongoDB.Bson;
using MongoDB.Driver;

// ReSharper disable InconsistentNaming
namespace MongoDB101.Tests
{
    public class AggregationTests : BaseTest
    {
        #region Group

        [Fact]
        public async Task GroupCountingWithBsonDocument()
        {
            BsonDocument groupProjection = new BsonDocument
            {
                { "_id", "$manufacturer" },
                { "num_products", new BsonDocument { { "$sum", 1 } } }
            };

            List<BsonDocument> productsWithCount = await inventoryContext.ProductsAsBson.Aggregate()
               .Group(groupProjection)
               .ToListAsync();

            productsWithCount.Should().HaveCount(5);
            productsWithCount.Should().Contain(x => x["_id"] == "Apple" && x["num_products"] == 4);
            productsWithCount.Should().Contain(x => x["_id"] == "Amazon" && x["num_products"] == 2);
        }

        [Fact]
        public async Task GroupCountingWithJSONString()
        {
            const string groupProjection = "{ _id: '$manufacturer', num_products: { $sum: 1 } }";

            List<BsonDocument> productsWithCount = await inventoryContext.ProductsAsBson.Aggregate()
                .Group(groupProjection)
                .ToListAsync();

            productsWithCount.Should().HaveCount(5);
            productsWithCount.Should().Contain(x => x["_id"] == "Apple" && x["num_products"] == 4);
            productsWithCount.Should().Contain(x => x["_id"] == "Amazon" && x["num_products"] == 2);
        }

        [Fact]
        public async Task GroupCountingWithExpressionTree()
        {
            var productsWithCount = await inventoryContext.Products.Aggregate()
                .Group(x => x.Manufacturer, g => new { Id = g.Key, NumberOfProducts = g.Sum(i => 1) })
                .ToListAsync();

            productsWithCount.Should().HaveCount(5);
            productsWithCount.Should().Contain(x => x.Id == "Apple" && x.NumberOfProducts == 4D);
            productsWithCount.Should().Contain(x => x.Id == "Amazon" && x.NumberOfProducts == 2D);
        }

        [Fact]
        public async Task CompoundGroupCountingWithBsonDocument()
        {
            BsonDocument groupProjection = new BsonDocument
            {
                {
                    "_id" ,
                    new BsonDocument
                    {
                        { "manufacturer", "$manufacturer" },
                        { "category", "$category" }
                    }
                },
                { "num_products", new BsonDocument { { "$sum", 1 } } }
            };

            List<BsonDocument> productsWithCount = await inventoryContext.ProductsAsBson.Aggregate()
               .Group(groupProjection)
               .ToListAsync();

            productsWithCount.Should().HaveCount(7);
            productsWithCount.Should().Contain(x => x["_id"]["manufacturer"] == "Apple" && x["_id"]["category"] == "Tablets" && x["num_products"] == 3);
            productsWithCount.Should().Contain(x => x["_id"]["manufacturer"] == "Amazon" && x["_id"]["category"] == "Tablets" && x["num_products"] == 2);
        }

        [Fact]
        public async Task CompoundGroupCountingWithJSONString()
        {
            const string groupProjection = "{ _id: { 'manufacturer': '$manufacturer', 'category': '$category' }, num_products: { $sum: 1 } }";

            List<BsonDocument> productsWithCount = await inventoryContext.ProductsAsBson.Aggregate()
                .Group(groupProjection)
                .ToListAsync();

            productsWithCount.Should().HaveCount(7);
            productsWithCount.Should().Contain(x => x["_id"]["manufacturer"] == "Apple" && x["_id"]["category"] == "Tablets" && x["num_products"] == 3);
            productsWithCount.Should().Contain(x => x["_id"]["manufacturer"] == "Amazon" && x["_id"]["category"] == "Tablets" && x["num_products"] == 2);
        }

        [Fact]
        public async Task CompundGroupCountingWithExpressionTree()
        {
            var productsWithCount = await inventoryContext.Products.Aggregate()
                .Group(x => new { x.Manufacturer, x.Category }, g => new { Id = g.Key, NumberOfProducts = g.Sum(i => 1) })
                .ToListAsync();

            productsWithCount.Should().HaveCount(7);
            productsWithCount.Should().Contain(x => x.Id.Manufacturer == "Apple" && x.Id.Category == "Tablets" && x.NumberOfProducts == 3D);
            productsWithCount.Should().Contain(x => x.Id.Manufacturer == "Amazon" && x.Id.Category == "Tablets" && x.NumberOfProducts == 2D);
        }

        #endregion ENDOF: Group
    }
}