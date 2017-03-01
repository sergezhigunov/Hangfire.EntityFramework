using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class HangfireDbContextTests
    {
        [Fact, CleanDatabase]
        public void Ctor()
        {
            string connectionString = ConnectionUtils.GetConnectionString();

            using (var context = new HangfireDbContext(connectionString, null))
            {
                Assert.NotNull(context.Counters);
                Assert.NotNull(context.DistributedLocks);
                Assert.NotNull(context.Hashes);
                Assert.NotNull(context.Jobs);
                Assert.NotNull(context.JobActualStates);
                Assert.NotNull(context.JobQueues);
                Assert.NotNull(context.JobParameters);
                Assert.NotNull(context.JobStates);
                Assert.NotNull(context.Lists);
                Assert.NotNull(context.Servers);
                Assert.NotNull(context.Sets);
            }
        }
    }
}