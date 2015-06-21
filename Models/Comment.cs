using System;

namespace MongoDB101.Models
{
    public class Comment
    {
        public string Author { get; set; }

        public string Content { get; set; }

        public DateTime CreatedAtUtc { get; set; }
    }
}