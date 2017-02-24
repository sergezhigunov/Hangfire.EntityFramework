using System.Data.Entity;

namespace Hangfire.EntityFramework
{
    internal class HangfireDbContext : DbContext
    {
        public HangfireDbContext(string nameOrConnectionString)
            : base(nameOrConnectionString)
        { }
    }
}
