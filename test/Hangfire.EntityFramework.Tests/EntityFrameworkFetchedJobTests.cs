// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    public class EntityFrameworkFetchedJobTests
    {
        private const string Queue = "QUEUE";
        private static readonly Guid JobId = Guid.NewGuid();

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkFetchedJob(Guid.Empty, Guid.Empty, null, Queue));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var storage = CreateStorage();

            Assert.Throws<ArgumentNullException>("queue",
                () => new EntityFrameworkFetchedJob(Guid.Empty, Guid.Empty, storage, null));
        }

        [Fact, RollbackTransaction]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            var storage = CreateStorage();
            var job = new HangfireJob { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, InvocationData = string.Empty, };
            var host = new HangfireServerHost { Id = EntityFrameworkJobStorage.ServerHostId, };
            var queueItem = new HangfireJobQueueItem { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, Job = job, Queue = Queue, };
            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
                context.JobQueueLookups.Add(new HangfireJobQueueItemLookup { QueueItem = queueItem, ServerHost = host, });
            });
            using (var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, JobId, storage, Queue))
            {
                Assert.Equal(JobId.ToString(), fetchedJob.JobId);
                Assert.Equal(Queue, fetchedJob.Queue);
            };
        }

        [Fact, RollbackTransaction]
        public void RemoveFromQueue_CorrectlyRemovesQueueItem()
        {
            var storage = CreateStorage();
            var job = new HangfireJob { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, InvocationData = string.Empty, };
            var host = new HangfireServerHost { Id = EntityFrameworkJobStorage.ServerHostId, };
            var queueItem = new HangfireJobQueueItem { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, Job = job, Queue = Queue, };
            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
                context.JobQueueLookups.Add(new HangfireJobQueueItemLookup { QueueItem = queueItem, ServerHost = host, });
            });
            using (var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, JobId, storage, Queue))
                fetchedJob.RemoveFromQueue();

            UseContext(context =>
            {
                Assert.False(context.JobQueueLookups.Any());
                Assert.False(context.JobQueues.Any());
            });
        }

        [Fact, RollbackTransaction]
        public void Requeue_CorrectlyReturnsItemBackToQueue()
        {
            var storage = CreateStorage();
            var job = new HangfireJob { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, InvocationData = string.Empty, };
            var host = new HangfireServerHost { Id = EntityFrameworkJobStorage.ServerHostId, };
            var queueItem = new HangfireJobQueueItem { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, Job = job, Queue = Queue, };
            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
                context.JobQueueLookups.Add(new HangfireJobQueueItemLookup { QueueItem = queueItem, ServerHost = host, });
            });
            using (var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, JobId, storage, Queue))
                fetchedJob.Requeue();

            UseContext(context =>
            {
                Assert.False(context.JobQueueLookups.Any());
                Assert.True(context.JobQueues.Any());
            });
        }

        [Fact, RollbackTransaction]
        public void Dispose_CorrectlyDisposeOwnedResources()
        {
            var storage = CreateStorage();
            var job = new HangfireJob { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, InvocationData = string.Empty, };
            var host = new HangfireServerHost { Id = EntityFrameworkJobStorage.ServerHostId, };
            var queueItem = new HangfireJobQueueItem { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, Job = job, Queue = Queue, };
            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
                context.JobQueueLookups.Add(new HangfireJobQueueItemLookup { QueueItem = queueItem, ServerHost = host, });
            });
            var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, JobId, storage, Queue);

            fetchedJob.Dispose();

            UseContext(context =>
            {
                Assert.False(context.JobQueueLookups.Any());
                Assert.True(context.JobQueues.Any());
            });
        }
    }
}
