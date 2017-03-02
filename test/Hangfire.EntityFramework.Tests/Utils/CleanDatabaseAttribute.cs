using System.Data.Entity;
using System.Reflection;
using System.Threading;
using System.Transactions;
using Xunit.Sdk;

namespace Hangfire.EntityFramework.Utils
{
    internal class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static object StaticLock { get; } = new object();

        private static bool Cleaned { get; set; }

        private IsolationLevel IsolationLevel { get; }

        private TransactionScope TransactionScope { get; set; }

        static CleanDatabaseAttribute()
        {
            Database.SetInitializer(new DropCreateDatabaseIfModelChanges<HangfireDbContext>());
        }

        public CleanDatabaseAttribute(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            IsolationLevel = IsolationLevel;
        }

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(StaticLock);
            if (!Cleaned)
            {
                CleanDatabase();
                Cleaned = true;
            }

            if (IsolationLevel != IsolationLevel.Unspecified)
            {
                TransactionScope = new TransactionScope(
                    TransactionScopeOption.RequiresNew,
                    new TransactionOptions { IsolationLevel = IsolationLevel });
            }

        }

        public override void After(MethodInfo methodUnderTest)
        {
            try
            {
                TransactionScope?.Dispose();
            }
            finally
            {
                Monitor.Exit(StaticLock);
            }
        }

        private static void CleanDatabase()
        {
            using (var context = new HangfireDbContext(ConnectionUtils.GetConnectionString(), nameof(Hangfire)))
            {
                context.Counters.RemoveRange(context.Counters);
                context.DistributedLocks.RemoveRange(context.DistributedLocks);
                context.Hashes.RemoveRange(context.Hashes);
                context.JobActualStates.RemoveRange(context.JobActualStates);
                context.JobStates.RemoveRange(context.JobStates);
                context.JobParameters.RemoveRange(context.JobParameters);
                context.JobQueues.RemoveRange(context.JobQueues);
                context.Jobs.RemoveRange(context.Jobs);
                context.Lists.RemoveRange(context.Lists);
                context.Servers.RemoveRange(context.Servers);
                context.Sets.RemoveRange(context.Sets);

                context.SaveChanges();
            }
        }
    }
}