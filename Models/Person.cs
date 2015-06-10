using MongoDB.Bson;

namespace MongoDB101.Models
{
    public class Person
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public string Profession { get; set; }
    }
}