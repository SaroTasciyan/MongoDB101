using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Models;

namespace MongoDB101.Context
{
    public class SchoolContext
    {
        public const string DatabaseName = "school";
        public const string StudentsCollectionName = "students";

        private readonly IMongoClient mongoClient;
        private IMongoDatabase database;

        private IMongoDatabase Database
        {
            get { return database ?? (database = mongoClient.GetDatabase(DatabaseName)); }
        }

        public IMongoCollection<BsonDocument> StudentsAsBson
        {
            get { return Database.GetCollection<BsonDocument>(StudentsCollectionName); }
        }

        public IMongoCollection<Student> Students
        {
            get { return Database.GetCollection<Student>(StudentsCollectionName); }
        }

        public SchoolContext(IMongoClient mongoClient)
        {
            this.mongoClient = mongoClient;
        }
        
        public async Task ResetData()
        {
            // # Delete existing data
            await Database.DropCollectionAsync(StudentsCollectionName);

            //TODO # Create new data
            // ...
        }
    }
}