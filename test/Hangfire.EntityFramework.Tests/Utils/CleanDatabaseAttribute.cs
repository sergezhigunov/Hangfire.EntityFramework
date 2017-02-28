using System.Data.Entity;
using System.Reflection;
using System.Threading;
using Xunit.Sdk;

namespace Hangfire.EntityFramework.Utils
{
    internal class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        static CleanDatabaseAttribute()
        {
            Database.SetInitializer(new DropCreateDatabaseIfModelChanges<HangfireDbContext>());
        }

        private static object StaticLock { get; } = new object();

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(StaticLock);
            CleanDatabase();
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Monitor.Exit(StaticLock);
        }

        private static void CleanDatabase()
        {
            using (var context = new HangfireDbContext(ConnectionUtils.GetConnectionString()))
            {
                context.Counters.RemoveRange(context.Counters);
                context.DistributedLocks.RemoveRange(context.DistributedLocks);
                context.Hashes.RemoveRange(context.Hashes);
                context.JobActualStates.RemoveRange(context.JobActualStates);
                context.JobStates.RemoveRange(context.JobStates);
                context.JobParameters.RemoveRange(context.JobParameters);
                context.Jobs.RemoveRange(context.Jobs);
                context.Lists.RemoveRange(context.Lists);
                context.Servers.RemoveRange(context.Servers);
                context.Sets.RemoveRange(context.Sets);

                context.SaveChanges();
            }
        }
    }
}