using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using MongoDB101.Models;

namespace MongoDB101.Context
{
    public class BlogContext
    {
        public const string DatabaseName = "blog";
        public const string PostsCollectionName = "posts";
        public const string UsersCollectionName = "users";

        private static readonly IEnumerable<User> usersData = new User[]
        {
            new User { Name = "Joel Spolsky", Email = null },
            new User { Name = "Jimmy Bogard", Email = null },
            new User { Name = "Vaughn Vernon", Email = null },
            new User { Name = "Jeff Atwood", Email = null },
            new User { Name = "Scott Hanselman", Email = null },
            new User { Name = "Martin Fowler", Email = "fowler@acm.org" },
            new User { Name = "Eric Lippert", Email = null },
            new User { Name = "Jon Skeet", Email = "skeet@pobox.com" },
        };

        private static readonly IEnumerable<Post> postsData = new Post[]
        {
            new Post
            {
                Author = "Joel Spolsky",
                Content = String.Empty,
                CreatedAtUtc = DateTime.UtcNow,
                Comments = null,
                Tags = new string[] { "Software Development" },
                Title = "12 Steps to Better Code"
            },
            new Post
            {
                Author = "Jon Skeet",
                Content = String.Empty,
                CreatedAtUtc = DateTime.UtcNow,
                Comments = null,
                Tags = new string[] { "Software Development", "Programming", ".NET", "LINQ" },
                Title = "Query Expression Syntax"
            },
            new Post
            {
                Author = "Eric Lippert",
                Content = String.Empty,
                CreatedAtUtc = DateTime.UtcNow,
                Comments = null,
                Tags = new string[] { "Software Development", "Programming", ".NET", "LINQ" },
                Title = "Computing a Cartesian Product"
            }
        };

        private readonly IMongoClient mongoClient;
        private IMongoDatabase database;

        private IMongoDatabase Database
        {
            get { return database ?? (database = mongoClient.GetDatabase(DatabaseName)); }
        }

        public IMongoCollection<BsonDocument> PostsAsBson
        {
            get { return Database.GetCollection<BsonDocument>(PostsCollectionName); }
        }

        public IMongoCollection<Post> Posts
        {
            get { return Database.GetCollection<Post>(PostsCollectionName); }
        }

        public IMongoCollection<BsonDocument> UsersAsBson
        {
            get { return Database.GetCollection<BsonDocument>(UsersCollectionName); }
        }

        public IMongoCollection<User> Users
        {
            get { return Database.GetCollection<User>(UsersCollectionName); }
        }

        public BlogContext(IMongoClient mongoClient)
        {
            this.mongoClient = mongoClient;
        }
        
        public async Task ResetData()
        {
            // # Delete existing data
            await Database.DropCollectionAsync(PostsCollectionName);
            await Database.DropCollectionAsync(UsersCollectionName);

            // # Create new data
            await Users.InsertManyAsync(usersData);
            await Posts.InsertManyAsync(postsData);

            // # Create new indexes
            await Users.Indexes.CreateOneAsync(Builders<User>.IndexKeys.Ascending(x => x.Name));
            await Posts.Indexes.CreateOneAsync(Builders<Post>.IndexKeys.Descending(x => x.CreatedAtUtc));
            await Posts.Indexes.CreateOneAsync(Builders<Post>.IndexKeys.Descending(x => x.Tags).Descending(x => x.CreatedAtUtc));
        }
    }
}