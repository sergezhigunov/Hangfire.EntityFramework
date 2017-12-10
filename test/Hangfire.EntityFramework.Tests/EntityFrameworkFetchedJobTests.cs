// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    [CleanDatabase]
    public class EntityFrameworkFetchedJobTests
    {
        private const string Queue = "QUEUE";

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkFetchedJob(0, 0, null, Queue));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var storage = CreateStorage();

            Assert.Throws<ArgumentNullException>("queue",
                () => new EntityFrameworkFetchedJob(0, 0, storage, null));
        }

        [Fact, RollbackTransaction]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            var storage = CreateStorage();

            var job = new HangfireJob
            {
                CreatedAt = DateTime.UtcNow,
            };

            var host = new HangfireServerHost
            {
                Id = EntityFrameworkJobStorage.ServerHostId,
            };

            var queueItem = new HangfireJobQueue
            {
                Job = job,
                Queue = Queue,
                ServerHost = host,
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
            });

            using (var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, job.Id, storage, Queue))
            {
                Assert.Equal(job.Id, fetchedJob.JobId);
                Assert.Equal(Queue, fetchedJob.Queue);
            };
        }

        [Fact, RollbackTransaction]
        public void RemoveFromQueue_CorrectlyRemovesQueueItem()
        {
            var storage = CreateStorage();

            var job = new HangfireJob
            {
                CreatedAt = DateTime.UtcNow,
            };

            var host = new HangfireServerHost
            {
                Id = EntityFrameworkJobStorage.ServerHostId,
            };

            var queueItem = new HangfireJobQueue
            {
                Job = job,
                Queue = Queue,
                ServerHost = host,
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
            });

            using (var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, job.Id, storage, Queue))
                fetchedJob.RemoveFromQueue();

            UseContext(context =>
                Assert.Empty(context.JobQueues));
        }

        [Fact, RollbackTransaction]
        public void Requeue_CorrectlyReturnsItemBackToQueue()
        {
            var storage = CreateStorage();

            var job = new HangfireJob
            {
                CreatedAt = DateTime.UtcNow,
            };

            var host = new HangfireServerHost
            {
                Id = EntityFrameworkJobStorage.ServerHostId,
            };

            var queueItem = new HangfireJobQueue
            {
                Job = job,
                Queue = Queue,
                ServerHost = host,
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
            });

            using (var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, job.Id, storage, Queue))
                fetchedJob.Requeue();

            UseContext(context =>
            {
                var actualItem = Assert.Single(context.JobQueues);
                Assert.Null(actualItem.ServerHostId);
            });
        }

        [Fact, RollbackTransaction]
        public void Dispose_CorrectlyDisposeOwnedResources()
        {
            var storage = CreateStorage();

            var job = new HangfireJob
            {
                CreatedAt = DateTime.UtcNow,
            };

            var host = new HangfireServerHost
            {
                Id = EntityFrameworkJobStorage.ServerHostId,
            };

            var queueItem = new HangfireJobQueue
            {
                Job = job,
                Queue = Queue,
                ServerHost = host,
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
            });

            var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, job.Id, storage, Queue);

            fetchedJob.Dispose();

            UseContext(context =>
            {
                var actualItem = Assert.Single(context.JobQueues);
                Assert.Null(actualItem.ServerHostId);
            });
        }
    }
}
