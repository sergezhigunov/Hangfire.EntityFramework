using Hangfire.EntityFramework.Utils;
using System;
using System.Linq;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class EntityFrameworkJobQueueMonitoringApiTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobQueueMonitoringApi(null));
        }

        [Fact, CleanDatabase]
        public void GetQueues_ReturnsEmptyCollection_WhenQueuedItemsNotExisits()
        {
            var api = CreateMonitoringApi();

            var queues = api.GetQueues();

            Assert.Equal(0, queues.Count());
        }

        [Fact, CleanDatabase]
        public void GetQueues_ReturnsAllGivenQueues_IfQueuesIsEmpty()
        {
            var date = DateTime.UtcNow;
            Guid[] jobIds = Enumerable.Repeat(0, 5).Select(x => Guid.NewGuid()).ToArray();
            var jobs = jobIds.Select(x => new HangfireJob
            {
                JobId = x,
                CreatedAt = DateTime.UtcNow,
                InvocationData = string.Empty
            });
            var jobQueueItems = jobIds.Select(x => new HangfireJobQueueItem
            {
                Id = Guid.NewGuid(),
                CreatedAt = date += new TimeSpan(0, 0, 1),
                Queue = x.ToString(),
                JobId = x
            });
            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobQueues.AddRange(jobQueueItems);
            });

            var api = CreateMonitoringApi();

            var queues = api.GetQueues();

            Assert.Equal(5, queues.Count());
            Assert.All(queues, queue => Assert.True(jobIds.Contains(Guid.Parse(queue))));
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ReturnsEmptyCollection_IfQueueIsEmpty()
        {
            string queue = Guid.NewGuid().ToString();
            var api = CreateMonitoringApi();

            var result = api.GetEnqueuedJobIds(queue, 5, 15);

            Assert.Empty(result);
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ReturnsCorrectResult()
        {
            var date = DateTime.UtcNow;
            string queue = Guid.NewGuid().ToString();
            Guid[] jobIds = Enumerable.Repeat(0, 10).Select(x => Guid.NewGuid()).ToArray();
            var jobs = jobIds.Select(x => new HangfireJob
            {
                JobId = x,
                CreatedAt = DateTime.UtcNow,
                InvocationData = string.Empty
            });
            var jobQueueItems = jobIds.Select(x => new HangfireJobQueueItem
            {
                Id = Guid.NewGuid(),
                CreatedAt = date += new TimeSpan(0, 0, 1),
                Queue = queue,
                JobId = x
            });

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobQueues.AddRange(jobQueueItems);
            });

            var api = CreateMonitoringApi();

            var result = api.GetEnqueuedJobIds(queue, 3, 2).ToArray();

            Assert.Equal(2, result.Length);
            Assert.Equal(jobIds[3], result[0]);
            Assert.Equal(jobIds[4], result[1]);
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ReturnsEmptyCollection()
        {
            string queue = Guid.NewGuid().ToString();
            var api = CreateMonitoringApi();

            var result = api.GetFetchedJobIds(queue, 5, 15);

            Assert.Empty(result);
        }

        [Fact, CleanDatabase]
        public void GetJobQueueCounters_ReturnsZeroes_WhenQueueNotExisits()
        {
            string queue = Guid.NewGuid().ToString();
            var api = CreateMonitoringApi();

            var result = api.GetJobQueueCounters(queue);

            Assert.NotNull(result);
            Assert.Equal(0, result.EnqueuedCount);
            Assert.Equal(0, result.FetchedCount);
        }

        [Fact, CleanDatabase]
        public void GetJobQueueCounters_ReturnsCorrectCounters()
        {
            var date = DateTime.UtcNow;
            string queue = Guid.NewGuid().ToString();
            Guid[] jobIds = Enumerable.Repeat(0, 3).Select(x => Guid.NewGuid()).ToArray();
            var jobs = jobIds.Select(x => new HangfireJob
            {
                JobId = x,
                CreatedAt = DateTime.UtcNow,
                InvocationData = string.Empty
            });
            var jobQueueItems = jobIds.Select(x => new HangfireJobQueueItem
            {
                Id = Guid.NewGuid(),
                CreatedAt = date += new TimeSpan(0, 0, 1),
                Queue = queue,
                JobId = x
            }).ToArray();
            jobQueueItems.Last().Queue = Guid.NewGuid().ToString();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobQueues.AddRange(jobQueueItems);
            });

            var api = CreateMonitoringApi();

            var result = api.GetJobQueueCounters(queue);

            Assert.NotNull(result);
            Assert.Equal(2, result.EnqueuedCount);
            Assert.Equal(0, result.FetchedCount);
        }

        private EntityFrameworkJobQueueMonitoringApi CreateMonitoringApi()
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            return new EntityFrameworkJobQueueMonitoringApi(storage);
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
