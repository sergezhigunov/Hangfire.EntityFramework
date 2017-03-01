using System;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class EntityFrameworkFetchedJobTests
    {
        private const string JobId = "id";
        private const string Queue = "queue";

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            UseContext(context =>
            {
                using (var transaction = context.Database.BeginTransaction())
                    Assert.Throws<ArgumentNullException>("context",
                        () => new EntityFrameworkFetchedJob(null, transaction, JobId, Queue));
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenTransactionIsNull()
        {
            UseContext(context => Assert.Throws<ArgumentNullException>("transaction",
                () => new EntityFrameworkFetchedJob(context, null, JobId, Queue)));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            UseContext(context =>
            {
                using (var transaction = context.Database.BeginTransaction())
                    Assert.Throws<ArgumentNullException>("jobId",
                        () => new EntityFrameworkFetchedJob(context, transaction, null, Queue));
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            UseContext(context =>
            {
                using (var transaction = context.Database.BeginTransaction())
                    Assert.Throws<ArgumentNullException>("queue",
                        () => new EntityFrameworkFetchedJob(context, transaction, JobId, null));
            });
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            UseContext(context =>
            {
                using (var transaction = context.Database.BeginTransaction())
                {
                    using (var fetchedJob = new EntityFrameworkFetchedJob(context, transaction, JobId, Queue))
                    {
                        Assert.Equal(JobId, fetchedJob.JobId);
                        Assert.Equal(Queue, fetchedJob.Queue);
                    }
                }
            });
        }

        private void UseContext(Action<HangfireDbContext> action)
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            storage.UseHangfireDbContext(action);
        }

        private T UseContext<T>(Func<HangfireDbContext, T> func)
        {
            T result = default(T);
            UseContext(context => { result = func(context); });
            return result;
        }

        private void UseContextWithSavingChanges(Action<HangfireDbContext> action)
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            storage.UseHangfireDbContext(context =>
            {
                action(context);
                context.SaveChanges();
            });
        }
    }
}
