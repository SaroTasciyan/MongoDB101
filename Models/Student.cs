
using System.Collections.Generic;

using MongoDB.Bson.Serialization.Attributes;

namespace MongoDB101.Models
{
    public class Student
    {
        public class Score
        {
            public string Type { get; set; }

            [BsonElement("score")]
            public double Point { get; set; }
        }

        public int Id { get; set; } 
        public string Name { get; set; }
        public ICollection<Score> Scores { get; set; }
    }
}