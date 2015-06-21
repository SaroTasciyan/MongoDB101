using System;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDB101.Models
{
    public class Person
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public string Profession { get; set; }
        public string[] Favorites { get; set; }

        public override string ToString()
        {
            return String.Format("Id: {0}, Name: '{1}', Age: {2}, Profession: '{3}'", Id, Name, Age, Profession);
        }
    }
}