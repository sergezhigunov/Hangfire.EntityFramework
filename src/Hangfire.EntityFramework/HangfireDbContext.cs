using System.Data.Entity;

namespace Hangfire.EntityFramework
{
    internal class HangfireDbContext : DbContext
    {
        public HangfireDbContext(string nameOrConnectionString)
            : base(nameOrConnectionString)
        { }

        public DbSet<HangfireCounter> Counters { get; set; }

        public DbSet<HangfireDistributedLock> DistributedLocks { get; set; }

        public DbSet<HangfireHash> Hashes { get; set; }

        public DbSet<HangfireJob> Jobs { get; set; }

        public DbSet<HangfireJobActualState> JobActualStates { get; set; }

        public DbSet<HangfireJobState> JobStates { get; set; }

        public DbSet<HangfireJobParameter> JobParameters { get; set; }

        public DbSet<HangfireListItem> Lists { get; set; }

        public DbSet<HangfireServer> Servers { get; set; }

        public DbSet<HangfireSet> Sets { get; set; }
    }
}
