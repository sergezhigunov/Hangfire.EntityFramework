// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using System.Transactions;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class EntityFrameworkFetchedJobTests
    {
        private const string Queue = "queue";
        private static readonly Guid JobId = Guid.NewGuid();

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
                        Assert.Equal(JobId.ToString(), fetchedJob.JobId);
                        Assert.Equal(Queue, fetchedJob.Queue);
                    }
                }
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_CorrectlyRemovesQueueItem()
        {
            var job = new HangfireJob { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, InvocationData = string.Empty, };
            var queueItem = new HangfireJobQueueItem { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, Job = job, Queue = Queue };
            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
            });
            var contextToUse = CreateContext();
            var transaction = contextToUse.Database.BeginTransaction();
            var fetchedQueueItem = (
                from item in contextToUse.JobQueues
                where item.Queue == Queue
                select item).
                FirstOrDefault();
            contextToUse.JobQueues.Remove(fetchedQueueItem);
            contextToUse.SaveChanges();

            using (var fetchedJob = new EntityFrameworkFetchedJob(contextToUse, transaction, fetchedQueueItem.JobId, Queue))
                fetchedJob.RemoveFromQueue();
        }

        [Fact, CleanDatabase]
        public void Requeue_CorrectlyReturnsItemBackToQueue()
        {
            var job = new HangfireJob { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, InvocationData = string.Empty, };
            var queueItem = new HangfireJobQueueItem { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, Job = job, Queue = Queue };
            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
            });
            var contextToUse = CreateContext();
            var transaction = contextToUse.Database.BeginTransaction();
            var fetchedQueueItem = (
                from item in contextToUse.JobQueues
                where item.Queue == Queue
                select item).
                FirstOrDefault();
            contextToUse.JobQueues.Remove(fetchedQueueItem);
            contextToUse.SaveChanges();

            using (var fetchedJob = new EntityFrameworkFetchedJob(contextToUse, transaction, fetchedQueueItem.JobId, Queue))
                fetchedJob.Requeue();
        }

        [Fact, CleanDatabase]
        public void Dispose_CorrectlyDisposeOwnedResources()
        {
            var job = new HangfireJob { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, InvocationData = string.Empty, };
            var queueItem = new HangfireJobQueueItem { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, Job = job, Queue = Queue };
            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
            });
            var contextToUse = CreateContext();
            var transaction = contextToUse.Database.BeginTransaction();
            var fetchedQueueItem = (
                from item in contextToUse.JobQueues
                where item.Queue == Queue
                select item).
                FirstOrDefault();
            contextToUse.JobQueues.Remove(fetchedQueueItem);
            contextToUse.SaveChanges();
            var fetchedJob = new EntityFrameworkFetchedJob(contextToUse, transaction, fetchedQueueItem.JobId, Queue);

            fetchedJob.Dispose();

            Assert.ThrowsAny<Exception>(() => transaction.Commit());
            Assert.Throws<InvalidOperationException>(() => contextToUse.SaveChanges());
        }

        private static void UseContext(Action<HangfireDbContext> action)
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            storage.UseHangfireDbContext(action);
        }

        private static T UseContext<T>(Func<HangfireDbContext, T> func)
        {
            T result = default(T);
            UseContext(context => { result = func(context); });
            return result;
        }

        private static void UseContextWithSavingChanges(Action<HangfireDbContext> action)
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            storage.UseHangfireDbContext(context =>
            {
                action(context);
                context.SaveChanges();
            });
        }

        private static HangfireDbContext CreateContext() =>
            new HangfireDbContext(ConnectionUtils.GetConnectionString(), nameof(Hangfire));
    }
}
