using System.Collections.Generic;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Models.Inventory;

namespace MongoDB101.Context
{
    public class InventoryContext : DbContext
    {
        public const string ProductsCollectionName = "products";

        private static readonly IEnumerable<Product> productsData = new Product[]
        {
            new Product { Name = "iPad 16GB Wifi", Manufacturer = "Apple", Category = "Tablets", Price = 499 },
            new Product { Name = "iPad 32GB Wifi", Manufacturer = "Apple", Category = "Tablets", Price = 599 },
            new Product { Name = "iPad 64GB Wifi", Manufacturer = "Apple", Category = "Tablets", Price = 699 },
            new Product { Name = "Galaxy S3", Manufacturer = "Samsung", Category = "Cell Phones", Price = 563 },
            new Product { Name = "Galaxy Tab 10", Manufacturer = "Samsung", Category = "Tablets", Price = 450.99D },
            new Product { Name = "Vaio", Manufacturer = "Sony", Category = "Laptops", Price = 499 },
            new Product { Name = "Macbook Air 13inch", Manufacturer = "Apple", Category = "Laptops", Price = 499 },
            new Product { Name = "Nexus 7", Manufacturer = "Google", Category = "Tablets", Price = 199 },
            new Product { Name = "Kindle Paper White", Manufacturer = "Amazon", Category = "Tablets", Price = 129 },
            new Product { Name = "Kindle Fire", Manufacturer = "Amazon", Category = "Tablets", Price = 199 }
        };

        protected override string DatabaseName
        {
            get { return "inventory"; }
        }

        public IMongoCollection<BsonDocument> ProductsAsBson
        {
            get { return Database.GetCollection<BsonDocument>(ProductsCollectionName); }
        }

        public IMongoCollection<Product> Products
        {
            get { return Database.GetCollection<Product>(ProductsCollectionName); }
        }

        public InventoryContext(IMongoClient mongoClient) : base(mongoClient) { }

        public override async Task ResetData()
        {
            // # Delete existing data
            await Database.DropCollectionAsync(ProductsCollectionName);

            // # Create new data
            await Products.InsertManyAsync(productsData);

            // # Create new indexes
            await Products.Indexes.CreateOneAsync(Builders<Product>.IndexKeys.Ascending(x => x.Manufacturer));
        }
    }
}