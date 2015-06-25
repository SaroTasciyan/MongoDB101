using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Models.School;

namespace MongoDB101.Context
{
    public class SchoolContext : DbContext
    {
        public const string StudentsCollectionName = "students";

        protected override string DatabaseName
        {
            get { return "school"; }
        }

        public IMongoCollection<BsonDocument> StudentsAsBson
        {
            get { return Database.GetCollection<BsonDocument>(StudentsCollectionName); }
        }

        public IMongoCollection<Student> Students
        {
            get { return Database.GetCollection<Student>(StudentsCollectionName); }
        }

        public SchoolContext(IMongoClient mongoClient) : base(mongoClient) { }

        public override async Task ResetData()
        {
            // # Delete existing data
            await Database.DropCollectionAsync(StudentsCollectionName);

            //TODO # Create new data
            // ...

            //TODO # Create new indexes
            // ...
        }
    }
}