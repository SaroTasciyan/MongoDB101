using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDB101.Models.Blog
{
    public class User
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        public string Name { get; set; }

        public string Email { get; set; }
    }
}