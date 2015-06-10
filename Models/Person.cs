using System;

using MongoDB.Bson;

namespace MongoDB101.Models
{
    public class Person
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public string Profession { get; set; }

        public override string ToString()
        {
            return String.Format("Id: {0}, Name: '{1}', Age: {2}, Profession: '{3}'", Id, Name, Age, Profession);
        }
    }
}