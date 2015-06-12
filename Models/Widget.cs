using System;

using MongoDB.Bson;

namespace MongoDB101.Models
{
    public class Widget
    {
        public ObjectId Id { get; set; }
        public int X { get; set; }

        public override string ToString()
        {
            return String.Format("Id: {0}, X: {1}", Id, X);
        }
    }
}