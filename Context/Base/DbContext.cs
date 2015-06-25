using System.Threading.Tasks;

using MongoDB.Driver;

namespace MongoDB101.Context
{
    public abstract class DbContext
    {
        protected abstract string DatabaseName { get; }

        private readonly IMongoClient mongoClient;
        private IMongoDatabase database;

        protected IMongoDatabase Database
        {
            get { return database ?? (database = mongoClient.GetDatabase(DatabaseName)); }
        }

        protected DbContext(IMongoClient mongoClient)
        {
            this.mongoClient = mongoClient;
        }

        public virtual async Task ResetData()
        {
            await new Task(() => { });
        }
    }
}