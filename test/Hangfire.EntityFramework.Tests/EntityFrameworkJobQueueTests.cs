// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    public class EntityFrameworkJobQueueTests
    {
        private static readonly string[] DefaultQueues = { "DEFAULT" };

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobQueue(null));
        }

        [Fact, RollbackTransaction]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            var queue = CreateJobQueue();

            var exception = Assert.Throws<ArgumentNullException>("queues",
                () => queue.Dequeue(null, CreateTimingOutCancellationToken()));
        }

        [Fact, RollbackTransaction]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            var queue = CreateJobQueue();

            var exception = Assert.Throws<ArgumentException>("queues",
                () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));
        }

        [Fact]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            var cts = new CancellationTokenSource();
            cts.Cancel();
            var queue = CreateJobQueue();

            Assert.Throws<OperationCanceledException>(
                () => queue.Dequeue(DefaultQueues, cts.Token));
        }

        [Fact, RollbackTransaction]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            var cts = new CancellationTokenSource(200);
            var queue = CreateJobQueue();

            Assert.Throws<OperationCanceledException>(
                () => queue.Dequeue(DefaultQueues, cts.Token));
        }

        [Fact, RollbackTransaction]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            Guid queueItemId = Guid.NewGuid();
            string queueName = DefaultQueues.First();

            var job = new HangfireJob
            {
                CreatedAt = DateTime.UtcNow,
            };

            var host = new HangfireServerHost
            {
                Id = EntityFrameworkJobStorage.ServerHostId,
            };

            var jobQueueItem = new HangfireJobQueueItem
            {
                Job = job,
                Queue = queueName,
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(jobQueueItem);
                context.ServerHosts.Add(host);
            });

            var queue = CreateJobQueue();

            var result = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

            Assert.NotNull(result);
            Assert.IsType<EntityFrameworkFetchedJob>(result);
            EntityFrameworkFetchedJob fetchedJob = (EntityFrameworkFetchedJob)result;
            Assert.Equal(job.Id, fetchedJob.JobId);
            Assert.Equal("DEFAULT", fetchedJob.Queue);
            var jobInQueue = UseContext(context => context.JobQueues.SingleOrDefault(x => x.Lookup == null));
            Assert.Null(jobInQueue);
        }

        [Fact, RollbackTransaction]
        public void Enqueue_AddsAJobToTheQueue()
        {
            string queueName = DefaultQueues.First();
            var job = new HangfireJob
            {
                CreatedAt = DateTime.UtcNow,
            };

            UseContextWithSavingChanges(context => context.Jobs.Add(job));

            var queue = CreateJobQueue();

            queue.Enqueue(queueName, job.Id.ToString(CultureInfo.InvariantCulture));

            var record = UseContext(context => context.JobQueues.Single());
            Assert.Equal(job.Id, record.JobId);
            Assert.Equal(queueName, record.Queue);
        }

        [Fact, RollbackTransaction]
        public void Enqueue_ShouldThrowAnException_WhenQueueIsNull()
        {
            var jobId = Guid.NewGuid().ToString();
            var queue = CreateJobQueue();

            Assert.Throws<ArgumentNullException>("queue",
                () => queue.Enqueue(null, jobId));
        }

        [Fact, RollbackTransaction]
        public void Enqueue_ShouldThrowAnException_WhenJobIdIsNull()
        {
            var queue = CreateJobQueue();

            Assert.Throws<ArgumentNullException>("jobId",
                () => queue.Enqueue("QUEUE", null));
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        private static EntityFrameworkJobQueue CreateJobQueue()
        {
            var storage = CreateStorage();
            return new EntityFrameworkJobQueue(storage);
        }
    }
}
