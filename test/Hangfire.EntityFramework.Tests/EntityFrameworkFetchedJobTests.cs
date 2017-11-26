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
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
                context.JobQueueLookups.Add(new HangfireJobQueueLookup
                {
                    QueueItem = queueItem,
                    ServerHost = host,
                });
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
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);
                context.JobQueueLookups.Add(new HangfireJobQueueLookup { QueueItem = queueItem, ServerHost = host, });
            });

            using (var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, job.Id, storage, Queue))
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
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);

                context.JobQueueLookups.Add(new HangfireJobQueueLookup
                {
                    QueueItem = queueItem,
                    ServerHost = host,
                });
            });
            using (var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, job.Id, storage, Queue))
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
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(queueItem);
                context.ServerHosts.Add(host);

                context.JobQueueLookups.Add(new HangfireJobQueueLookup
                {
                    QueueItem = queueItem,
                    ServerHost = host,
                });
            });

            var fetchedJob = new EntityFrameworkFetchedJob(queueItem.Id, job.Id, storage, Queue);

            fetchedJob.Dispose();

            UseContext(context =>
            {
                Assert.False(context.JobQueueLookups.Any());
                Assert.True(context.JobQueues.Any());
            });
        }
    }
}
