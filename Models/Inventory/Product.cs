using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDB101.Models.Inventory
{
    public class Product
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }
        public string Name { get; set; }
        public string Manufacturer { get; set; }
        public string Category { get; set; }
        public double Price { get; set; }
    }
}