using System;
using System.Linq;
using System.Threading;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class EntityFrameworkJobQueueTests
    {
        private static readonly string[] DefaultQueues = { "default" };

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobQueue(null));
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            var queue = CreateJobQueue();

            var exception = Assert.Throws<ArgumentNullException>("queues",
                () => queue.Dequeue(null, CreateTimingOutCancellationToken()));
        }

        [Fact, CleanDatabase]
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

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            var cts = new CancellationTokenSource(200);
            var queue = CreateJobQueue();

            Assert.Throws<OperationCanceledException>(
                () => queue.Dequeue(DefaultQueues, cts.Token));
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            Guid jobId = Guid.NewGuid();
            Guid queueItemId = Guid.NewGuid();
            string queueName = DefaultQueues.First();
            var job = new HangfireJob
            {
                JobId = jobId,
                CreatedAt = DateTime.UtcNow,
                InvocationData = string.Empty,
                Arguments = string.Empty,
            };
            var jobQueueItem = new HangfireJobQueueItem
            {
                Id = queueItemId,
                CreatedAt = DateTime.UtcNow,
                JobId = jobId,
                Queue = queueName,
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobQueues.Add(jobQueueItem);
            });

            var queue = CreateJobQueue();

            var result = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

            Assert.NotNull(result);
            Assert.IsType<EntityFrameworkFetchedJob>(result);
            EntityFrameworkFetchedJob fetchedJob = (EntityFrameworkFetchedJob)result;
            Assert.Equal(jobId.ToString(), fetchedJob.JobId);
            Assert.Equal("default", fetchedJob.Queue);
            var jobInQueue = UseContext(context => context.JobQueues.SingleOrDefault());
            Assert.Null(jobInQueue);
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            Guid jobId = Guid.NewGuid();
            string queueName = DefaultQueues.First();
            var job = new HangfireJob
            {
                JobId = jobId,
                CreatedAt = DateTime.UtcNow,
                InvocationData = string.Empty,
                Arguments = string.Empty,
            };

            UseContextWithSavingChanges(context => context.Jobs.Add(job));

            var queue = CreateJobQueue();

            queue.Enqueue(queueName, jobId.ToString());

            var record = UseContext(context => context.JobQueues.Single());
            Assert.Equal(jobId, record.JobId);
            Assert.Equal(queueName, record.Queue);
            Assert.Null(record.FetchedAt);
        }

        [Fact, CleanDatabase]
        public void Enqueue_ShouldThrowAnException_WhenQueueIsNull()
        {
            var jobId = Guid.NewGuid().ToString();
            var queue = CreateJobQueue();

            Assert.Throws<ArgumentNullException>("queue",
                () => queue.Enqueue(null, jobId));
        }

        [Fact, CleanDatabase]
        public void Enqueue_ShouldThrowAnException_WhenJobIdIsNull()
        {
            var queue = CreateJobQueue();

            Assert.Throws<ArgumentNullException>("jobId",
                () => queue.Enqueue("queue", null));
        }

        private void UseContextWithSavingChanges(Action<HangfireDbContext> action)
        {
            using (var context = new HangfireDbContext(ConnectionUtils.GetConnectionString()))
            {
                action(context);
                context.SaveChanges();
            }
        }

        private T UseContext<T>(Func<HangfireDbContext,T> func)
        {
            using (var context = new HangfireDbContext(ConnectionUtils.GetConnectionString()))
                return func(context);
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        public static void Sample(string arg1, string arg2) { }

        private static EntityFrameworkJobQueue CreateJobQueue()
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            return new EntityFrameworkJobQueue(storage);
        }
    }
}
