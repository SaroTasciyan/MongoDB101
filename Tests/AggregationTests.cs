using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Xunit;
using FluentAssertions;

using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Models.Blog;
using MongoDB101.Models.Inventory;

// ReSharper disable InconsistentNaming
namespace MongoDB101.Tests
{
    //TODO : İsimlendirme ve alan isimlerini düzelt!
    public class AggregationTests : BaseTest
    {
        #region Group - Count

        [Fact]
        public async Task GroupCountWithBsonDocument()
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
        public async Task GroupCountWithJSONString()
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
        public async Task GroupCountWithExpressionTree()
        {
            var productsWithCount = await inventoryContext.Products.Aggregate()
                .Group(x => x.Manufacturer, g => new { Id = g.Key, NumberOfProducts = g.Sum(i => 1) })
                .ToListAsync();

            productsWithCount.Should().HaveCount(5);
            productsWithCount.Should().Contain(x => x.Id == "Apple" && x.NumberOfProducts == 4D);
            productsWithCount.Should().Contain(x => x.Id == "Amazon" && x.NumberOfProducts == 2D);
        }

        [Fact]
        public async Task CompoundGroupCountWithBsonDocument()
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
        public async Task CompoundGroupCountWithJSONString()
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
        public async Task CompundGroupCountWithExpressionTree()
        {
            var productsWithCount = await inventoryContext.Products.Aggregate()
                .Group(x => new { x.Manufacturer, x.Category }, g => new { Id = g.Key, NumberOfProducts = g.Sum(i => 1) })
                .ToListAsync();

            productsWithCount.Should().HaveCount(7);
            productsWithCount.Should().Contain(x => x.Id.Manufacturer == "Apple" && x.Id.Category == "Tablets" && x.NumberOfProducts == 3D);
            productsWithCount.Should().Contain(x => x.Id.Manufacturer == "Amazon" && x.Id.Category == "Tablets" && x.NumberOfProducts == 2D);
        }

        #endregion ENDOF: Group - Count

        #region Group - Sum

        [Fact]
        public async Task GroupSumWithBsonDocument()
        {
            BsonDocument groupProjection = new BsonDocument
            {
                { "_id", "$manufacturer" },
                { "sum_prices", new BsonDocument { { "$sum", "$price" } } }
            };

            List<BsonDocument> productsWithSumPrices = await inventoryContext.ProductsAsBson.Aggregate()
               .Group(groupProjection)
               .ToListAsync();

            productsWithSumPrices.Should().HaveCount(5);
            productsWithSumPrices.Should().Contain(x => x["_id"] == "Apple" && x["sum_prices"] == 2296);
            productsWithSumPrices.Should().Contain(x => x["_id"] == "Amazon" && x["sum_prices"] == 328);
        }

        [Fact]
        public async Task GroupSumWithJSONString()
        {
            const string groupProjection = "{ _id: '$manufacturer', sum_prices: { $sum: '$price' } }";

            List<BsonDocument> productsWithSumPrices = await inventoryContext.ProductsAsBson.Aggregate()
                .Group(groupProjection)
                .ToListAsync();

            productsWithSumPrices.Should().HaveCount(5);
            productsWithSumPrices.Should().Contain(x => x["_id"] == "Apple" && x["sum_prices"] == 2296);
            productsWithSumPrices.Should().Contain(x => x["_id"] == "Amazon" && x["sum_prices"] == 328);
        }

        [Fact]
        public async Task GroupSumWithExpressionTree()
        {
            var productsWithSumPrices = await inventoryContext.Products.Aggregate()
                .Group(x => x.Manufacturer, g => new { Id = g.Key, SumPrices = g.Sum(i => i.Price) })
                .ToListAsync();

            productsWithSumPrices.Should().HaveCount(5);
            productsWithSumPrices.Should().Contain(x => x.Id == "Apple" && x.SumPrices == 2296);
            productsWithSumPrices.Should().Contain(x => x.Id == "Amazon" && x.SumPrices == 328);
        }

        #endregion ENDOF: Group Sum

        #region Group - Average

        [Fact]
        public async Task GroupAverageWithBsonDocument()
        {
            BsonDocument groupProjection = new BsonDocument
            {
                { "_id", "$category" },
                { "avg_prices", new BsonDocument { { "$avg", "$price" } } }
            };

            List<BsonDocument> productsWithAveragePrices = await inventoryContext.ProductsAsBson.Aggregate()
               .Group(groupProjection)
               .ToListAsync();

            productsWithAveragePrices.Should().HaveCount(3);
            productsWithAveragePrices.Should().Contain(x => x["_id"] == "Laptops" && x["avg_prices"] == 499);
            productsWithAveragePrices.Should().Contain(x => x["_id"] == "Cell Phones" && x["avg_prices"] == 563);
        }

        [Fact]
        public async Task GroupAverageWithJSONString()
        {
            const string groupProjection = "{ _id: '$category', avg_prices: { $avg: '$price' } }";

            List<BsonDocument> productsWithAveragePrices = await inventoryContext.ProductsAsBson.Aggregate()
                .Group(groupProjection)
                .ToListAsync();

            productsWithAveragePrices.Should().HaveCount(3);
            productsWithAveragePrices.Should().Contain(x => x["_id"] == "Laptops" && x["avg_prices"] == 499);
            productsWithAveragePrices.Should().Contain(x => x["_id"] == "Cell Phones" && x["avg_prices"] == 563);
        }

        [Fact]
        public async Task GroupAverageWithExpressionTree()
        {
            var productsWithSumPrices = await inventoryContext.Products.Aggregate()
                .Group(x => x.Category, g => new { Id = g.Key, AveragePrices = g.Average(i => i.Price) })
                .ToListAsync();

            productsWithSumPrices.Should().HaveCount(3);
            productsWithSumPrices.Should().Contain(x => x.Id == "Laptops" && x.AveragePrices == 499);
            productsWithSumPrices.Should().Contain(x => x.Id == "Cell Phones" && x.AveragePrices == 563);
        }

        #endregion ENDOF: Group - Average

        #region Group - Max

        [Fact]
        public async Task GroupMaxWithBsonDocument()
        {
            BsonDocument groupProjection = new BsonDocument
            {
                { "_id", "$category" },
                { "max_price", new BsonDocument { { "$max", "$price" } } }
            };

            List<BsonDocument> categoriesWithMaxPrices = await inventoryContext.ProductsAsBson.Aggregate()
               .Group(groupProjection)
               .ToListAsync();

            categoriesWithMaxPrices.Should().HaveCount(3);
            categoriesWithMaxPrices.Should().Contain(x => x["_id"] == "Laptops" && x["max_price"] == 499);
            categoriesWithMaxPrices.Should().Contain(x => x["_id"] == "Tablets" && x["max_price"] == 699);
        }

        [Fact]
        public async Task GroupMaxWithJSONString()
        {
            const string groupProjection = "{ _id: '$category', max_price: { $max: '$price' } }";

            List<BsonDocument> categoriesWithMaxPrices = await inventoryContext.ProductsAsBson.Aggregate()
                .Group(groupProjection)
                .ToListAsync();

            categoriesWithMaxPrices.Should().HaveCount(3);
            categoriesWithMaxPrices.Should().Contain(x => x["_id"] == "Laptops" && x["max_price"] == 499);
            categoriesWithMaxPrices.Should().Contain(x => x["_id"] == "Tablets" && x["max_price"] == 699);
        }

        [Fact]
        public async Task GroupMaxWithExpressionTree()
        {
            var categoriesWithMaxPrices = await inventoryContext.Products.Aggregate()
                .Group(x => x.Category, g => new { Id = g.Key, MaxPrice = g.Max(i => i.Price) })
                .ToListAsync();

            categoriesWithMaxPrices.Should().HaveCount(3);
            categoriesWithMaxPrices.Should().Contain(x => x.Id == "Laptops" && x.MaxPrice == 499);
            categoriesWithMaxPrices.Should().Contain(x => x.Id == "Tablets" && x.MaxPrice == 699);
        }

        #endregion ENDOF: Group - Max

        #region Group - Min

        [Fact]
        public async Task GroupMinWithBsonDocument()
        {
            BsonDocument groupProjection = new BsonDocument
            {
                { "_id", "$category" },
                { "min_price", new BsonDocument { { "$min", "$price" } } }
            };

            List<BsonDocument> categoriesWithMinPrices = await inventoryContext.ProductsAsBson.Aggregate()
               .Group(groupProjection)
               .ToListAsync();

            categoriesWithMinPrices.Should().HaveCount(3);
            categoriesWithMinPrices.Should().Contain(x => x["_id"] == "Laptops" && x["min_price"] == 499);
            categoriesWithMinPrices.Should().Contain(x => x["_id"] == "Tablets" && x["min_price"] == 129);
        }

        [Fact]
        public async Task GroupMinWithJSONString()
        {
            const string groupProjection = "{ _id: '$category', min_price: { $min: '$price' } }";

            List<BsonDocument> categoriesWithMinPrices = await inventoryContext.ProductsAsBson.Aggregate()
                .Group(groupProjection)
                .ToListAsync();

            categoriesWithMinPrices.Should().HaveCount(3);
            categoriesWithMinPrices.Should().Contain(x => x["_id"] == "Laptops" && x["min_price"] == 499);
            categoriesWithMinPrices.Should().Contain(x => x["_id"] == "Tablets" && x["min_price"] == 129);
        }

        [Fact]
        public async Task GroupMinWithExpressionTree()
        {
            var categoriesWithMinPrices = await inventoryContext.Products.Aggregate()
                .Group(x => x.Category, g => new { Id = g.Key, MinPrice = g.Min(i => i.Price) })
                .ToListAsync();

            categoriesWithMinPrices.Should().HaveCount(3);
            categoriesWithMinPrices.Should().Contain(x => x.Id == "Laptops" && x.MinPrice == 499);
            categoriesWithMinPrices.Should().Contain(x => x.Id == "Tablets" && x.MinPrice == 129);
        }

        #endregion ENDOF: Group - Min

        #region Group - AddToSet

        [Fact]
        public async Task GroupAddToSetWithBsonDocument()
        {
            BsonDocument groupProjection = new BsonDocument
            {
                { "_id", "$manufacturer" },
                { "categories", new BsonDocument { { "$addToSet", "$category" } } }
            };

            List<BsonDocument> productsWithCategories = await inventoryContext.ProductsAsBson.Aggregate()
               .Group(groupProjection)
               .ToListAsync();

            productsWithCategories.Should().HaveCount(5);
            
            BsonDocument productsOfApple = productsWithCategories.Single(x => x["_id"] == "Apple");
            productsOfApple["categories"].AsBsonArray.Should().HaveCount(2);
            productsOfApple["categories"].AsBsonArray.Contains("Tablets");
            productsOfApple["categories"].AsBsonArray.Contains("Laptops");
            
            BsonDocument productsOfAmazon = productsWithCategories.Single(x => x["_id"] == "Amazon");
            productsOfAmazon["categories"].AsBsonArray.Should().HaveCount(1);
            productsOfAmazon["categories"].AsBsonArray.Contains("Tablets");
        }

        [Fact]
        public async Task GroupAddToSetWithJSONString()
        {
            const string groupProjection = "{ _id: '$manufacturer', categories: { $addToSet: '$category' } }";

            List<BsonDocument> productsWithCategories = await inventoryContext.ProductsAsBson.Aggregate()
                .Group(groupProjection)
                .ToListAsync();

            productsWithCategories.Should().HaveCount(5);

            BsonDocument productsOfApple = productsWithCategories.Single(x => x["_id"] == "Apple");
            productsOfApple["categories"].AsBsonArray.Should().HaveCount(2);
            productsOfApple["categories"].AsBsonArray.Contains("Tablets");
            productsOfApple["categories"].AsBsonArray.Contains("Laptops");

            BsonDocument productsOfAmazon = productsWithCategories.Single(x => x["_id"] == "Amazon");
            productsOfAmazon["categories"].AsBsonArray.Should().HaveCount(1);
            productsOfAmazon["categories"].AsBsonArray.Contains("Tablets");
        }

        #endregion ENDOF: Group - AddToSet

        #region Group - Push

        [Fact]
        public async Task GroupPushWithBsonDocument()
        {
            BsonDocument groupProjection = new BsonDocument
            {
                { "_id", "$manufacturer" },
                { "categories", new BsonDocument { { "$push", "$category" } } }
            };

            List<BsonDocument> productsWithCategories = await inventoryContext.ProductsAsBson.Aggregate()
               .Group(groupProjection)
               .ToListAsync();

            productsWithCategories.Should().HaveCount(5);

            BsonDocument productsOfApple = productsWithCategories.Single(x => x["_id"] == "Apple");
            productsOfApple["categories"].AsBsonArray.Should().HaveCount(4); // # Duplicated elements
            productsOfApple["categories"].AsBsonArray.Contains("Tablets");
            productsOfApple["categories"].AsBsonArray.Contains("Laptops");

            BsonDocument productsOfAmazon = productsWithCategories.Single(x => x["_id"] == "Amazon");
            productsOfAmazon["categories"].AsBsonArray.Should().HaveCount(2); // # Duplicated elements
            productsOfAmazon["categories"].AsBsonArray.Contains("Tablets");
        }

        [Fact]
        public async Task GroupPushWithJSONString()
        {
            const string groupProjection = "{ _id: '$manufacturer', categories: { $push: '$category' } }";

            List<BsonDocument> productsWithCategories = await inventoryContext.ProductsAsBson.Aggregate()
                .Group(groupProjection)
                .ToListAsync();

            productsWithCategories.Should().HaveCount(5);

            BsonDocument productsOfApple = productsWithCategories.Single(x => x["_id"] == "Apple");
            productsOfApple["categories"].AsBsonArray.Should().HaveCount(4);
            productsOfApple["categories"].AsBsonArray.Contains("Tablets");
            productsOfApple["categories"].AsBsonArray.Contains("Laptops");

            BsonDocument productsOfAmazon = productsWithCategories.Single(x => x["_id"] == "Amazon");
            productsOfAmazon["categories"].AsBsonArray.Should().HaveCount(2);
            productsOfAmazon["categories"].AsBsonArray.Contains("Tablets");
        }

        #endregion ENDOF: Group - Push

        #region Project

        [Fact]
        public async Task ProjectWithBsonDocument()
        {
            BsonDocument groupProjection = new BsonDocument
            {
                { "_id", 0 },
                { "maker", new BsonDocument { { "$toLower", "$manufacturer" } } },
                {
                    "details", 
                    new BsonDocument
                    {
                        { "category", "$category" },
                        { "price", new BsonDocument { { "$multiply", new BsonArray { "$price", 10 } } } },
                    }
                },
                { "item", "$name" },
            };
            
            List<BsonDocument> products = await inventoryContext.ProductsAsBson.Aggregate()
               .Project(groupProjection)
               .ToListAsync();

            products.Should().HaveCount(10);
            products.First()["maker"].Should().Be("apple");
            products.First()["details"]["price"].Should().Be(4990);
            products.First()["item"].Should().Be("iPad 16GB Wifi");

            products.Last()["maker"].Should().Be("amazon");
            products.Last()["details"]["price"].Should().Be(1990);
            products.Last()["item"].Should().Be("Kindle Fire");
        }

        [Fact]
        public async Task ProjectWithExpressionTree()
        {
            var products = await inventoryContext.Products.Aggregate().Project
            (
                x => new
                {
                    Maker = x.Manufacturer.ToLower(),
                    Details = new { Category = x.Category, Price = x.Price * 10 },
                    Item = x.Name
                }
            )
            .ToListAsync();

            products.Should().HaveCount(10);
            products.First().Maker.Should().Be("apple");
            products.First().Details.Price.Should().Be(4990);
            products.First().Item.Should().Be("iPad 16GB Wifi");

            products.Last().Maker.Should().Be("amazon");
            products.Last().Details.Price.Should().Be(1990);
            products.Last().Item.Should().Be("Kindle Fire");
        }

        #endregion ENDOF: Project

        #region Match

        [Fact]
        public async Task MatchWithBsonDocument()
        {
            BsonDocument filter = new BsonDocument { { "manufacturer", "Apple" } };

            List<BsonDocument> products = await inventoryContext.ProductsAsBson.Aggregate()
               .Match(filter)
               .ToListAsync();

            products.Should().HaveCount(4);
            products.First()["manufacturer"].Should().Be("Apple");
            products.Last()["manufacturer"].Should().Be("Apple");
        }

        [Fact]
        public async Task MatchWithExpressionTree()
        {
            List<Product> products = await inventoryContext.Products.Aggregate()
                .Match(x => x.Manufacturer == "Apple")
                .ToListAsync();

            products.Should().HaveCount(4);
            products.First().Manufacturer.Should().Be("Apple");
            products.Last().Manufacturer.Should().Be("Apple");
        }

        #endregion ENDOF: Match

        #region Sort

        [Fact]
        public async Task SortWithBsonDocument()
        {
            BsonDocument sortDefinition = new BsonDocument { { "price", -1 } };

            List<BsonDocument> products = await inventoryContext.ProductsAsBson.Aggregate()
               .Sort(sortDefinition)
               .ToListAsync();

            products.Should().HaveCount(10);
            products.First()["price"].Should().Be(699);
            products.Last()["price"].Should().Be(129);
        }

        [Fact]
        public async Task SortWithExpressionTree()
        {
            List<Product> products = await inventoryContext.Products.Aggregate()
                .SortByDescending(x => x.Price)
                .ToListAsync();

            products.Should().HaveCount(10);
            products.First().Price.Should().Be(699);
            products.Last().Price.Should().Be(129);
        }


        [Fact]
        public async Task SortMultiFieldsWithBsonDocument()
        {
            BsonDocument sortDefinition = new BsonDocument
            {
                { "category", 1 },
                { "price", -1 }
            };

            List<BsonDocument> products = await inventoryContext.ProductsAsBson.Aggregate()
               .Sort(sortDefinition)
               .ToListAsync();

            products.Should().HaveCount(10);
            products.First()["category"].Should().Be("Cell Phones");
            products.First()["price"].Should().Be(563);
            products.Last()["category"].Should().Be("Tablets");
            products.Last()["price"].Should().Be(129);
        }

        [Fact]
        public async Task SortMultiFieldWithExpressionTree()
        {
            List<Product> products = await inventoryContext.Products.Aggregate()
                .SortBy(x => x.Category)
                .ThenByDescending(x => x.Price)
                .ToListAsync();

            products.Should().HaveCount(10);
            products.First().Category.Should().Be("Cell Phones");
            products.First().Price.Should().Be(563);
            products.Last().Category.Should().Be("Tablets");
            products.Last().Price.Should().Be(129);
        }

        #endregion ENDOF: Sort

        #region Skip and Limit

        [Fact]
        public async Task SkipAndLimitWithBsonDocument()
        {
            BsonDocument sortDefinition = new BsonDocument { { "price", -1 } };

            List<BsonDocument> products = await inventoryContext.ProductsAsBson.Aggregate()
               .Sort(sortDefinition)
               .Skip(1)
               .Limit(1)
               .ToListAsync();

            products.Should().HaveCount(1);
            products.First()["price"].Should().Be(599);
        }

        [Fact]
        public async Task SkipAndLimitWithExpressionTree()
        {
            List<Product> products = await inventoryContext.Products.Aggregate()
                .SortByDescending(x => x.Price)
                .Skip(1)
                .Limit(1)
                .ToListAsync();

            products.Should().HaveCount(1);
            products.First().Price.Should().Be(599);
        }

        #endregion ENDOF: Skip and Limit

        #region First and Last

        [Fact]
        public async Task FirstWithBsonDocument()
        {
            BsonDocument sortDefinition = new BsonDocument
            {
                { "category", 1 },
                { "price", -1 }
            };

            BsonDocument group = new BsonDocument
            {
                { "_id", "$category" },
                { "price", new BsonDocument { { "$first", "$price" } } },
            };

            List<BsonDocument> products = await inventoryContext.ProductsAsBson.Aggregate()
               .Sort(sortDefinition)
               .Group(group)
               .ToListAsync();

            products.Should().HaveCount(3);
            products.First()["_id"].Should().Be("Tablets");
            products.First()["price"].Should().Be(699);
            products.Last()["_id"].Should().Be("Cell Phones");
            products.Last()["price"].Should().Be(563);
        }

        [Fact]
        public async Task FirstWithExpressionTree()
        {
            var products = await inventoryContext.Products.Aggregate()
                .SortBy(x => x.Category)
                .ThenByDescending(x => x.Price)
                .Group(x => x.Category, g => new { Id = g.Key, Price = g.First().Price })
                .ToListAsync();

            products.Should().HaveCount(3);
            products.First().Id.Should().Be("Tablets");
            products.First().Price.Should().Be(699);
            products.Last().Id.Should().Be("Cell Phones");
            products.Last().Price.Should().Be(563);
        }

        [Fact]
        public async Task LastWithBsonDocument()
        {
            BsonDocument sortDefinition = new BsonDocument
            {
                { "category", 1 },
                { "price", -1 }
            };

            BsonDocument group = new BsonDocument
            {
                { "_id", "$category" },
                { "price", new BsonDocument { { "$last", "$price" } } },
            };

            List<BsonDocument> products = await inventoryContext.ProductsAsBson.Aggregate()
               .Sort(sortDefinition)
               .Group(group)
               .ToListAsync();

            products.Should().HaveCount(3);
            products.First()["_id"].Should().Be("Tablets");
            products.First()["price"].Should().Be(129);
            products.Last()["_id"].Should().Be("Cell Phones");
            products.Last()["price"].Should().Be(563);
        }

        [Fact]
        public async Task LastWithExpressionTree()
        {
            var products = await inventoryContext.Products.Aggregate()
                .SortBy(x => x.Category)
                .ThenByDescending(x => x.Price)
                .Group(x => x.Category, g => new { Id = g.Key, Price = g.Last().Price })
                .ToListAsync();

            products.Should().HaveCount(3);
            products.First().Id.Should().Be("Tablets");
            products.First().Price.Should().Be(129);
            products.Last().Id.Should().Be("Cell Phones");
            products.Last().Price.Should().Be(563);
        }

        #endregion ENDOF: First and Last

        #region Unwind

        [Fact]
        public async Task UnwindWithBsonDocument()
        {
            List<BsonDocument> posts = await blogContext.PostsAsBson.Aggregate()
               .Unwind("tags")
               .ToListAsync();

            posts.Should().HaveCount(9);
            posts.First()["author"].Should().Be("Joel Spolsky");
            posts.First()["tags"].Should().Be("Software Development");
            posts.Last()["author"].Should().Be("Eric Lippert");
            posts.Last()["tags"].Should().Be("LINQ");
        }

        [Fact(Skip="Does not work: System.Linq First() method is not supported for projection operations")]
        public async Task UnwindWithExpressionTree()
        {
            var posts = await blogContext.Posts.Aggregate()
               .Unwind<Post, Post>(x => x.Tags)
               .Project(x => new { Author = x.Author, Tag = x.Tags.First() })
               .ToListAsync();

            posts.Should().HaveCount(9);
            posts.First().Author.Should().Be("Joel Spolsky");
            posts.First().Tag.Should().Be("Software Development");
            posts.Last().Author.Should().Be("Eric Lippert");
            posts.Last().Tag.Should().Be("LINQ");
        }

        #endregion ENDOF: Unwind
    }
}