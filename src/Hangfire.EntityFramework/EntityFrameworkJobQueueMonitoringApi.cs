using System;
using System.Data.Entity;
using System.Linq;
using Hangfire.Annotations;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private EntityFrameworkJobStorage Storage { get; }

        public EntityFrameworkJobQueueMonitoringApi([NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            Storage = storage;
        }

        public string[] GetQueues()
        {
            return Storage.UseHangfireDbContext(context => (
                from queueItem in context.JobQueues
                select queueItem.Queue).
                Distinct().
                ToArray());
        }

        public Guid[] GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return Storage.UseHangfireDbContext(context => (
                from item in context.JobQueues
                where item.Queue == queue
                orderby item.CreatedAt ascending
                select item.Id).
                Skip(() => from).
                Take(() => perPage).
                ToArray());
        }

        public Guid[] GetFetchedJobIds(string queue, int from, int perPage) => new Guid[0];

        public JobQueueCounters GetJobQueueCounters(string queue)
        {
            long enqueuedCount = Storage.UseHangfireDbContext(context =>
                context.JobQueues.Count(x => x.Queue == queue));

            return new JobQueueCounters { EnqueuedCount = enqueuedCount, };
        }
    }
}