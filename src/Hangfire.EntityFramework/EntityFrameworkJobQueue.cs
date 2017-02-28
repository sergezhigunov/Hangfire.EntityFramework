using System;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobQueue : IPersistentJobQueue
    {
        public EntityFrameworkJobStorage Storage { get; }

        public EntityFrameworkJobQueue([NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            Storage = storage;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException(ErrorStrings.QueuesCannotBeEmpty, nameof(queues));

            HangfireJobQueueItem fetchedJob = null;
            DbContextTransaction transaction = null;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var context = Storage.CreateHangfireDbContext();
                try
                {
                    transaction = context.Database.BeginTransaction(IsolationLevel.ReadCommitted);

                    fetchedJob = (
                        from item in context.JobQueues
                        where queues.Contains(item.Queue)
                        select item).
                        FirstOrDefault();

                    if (fetchedJob != null)
                        context.JobQueues.Remove(fetchedJob);


                   if (fetchedJob != null)
                    {
                        context.JobQueues.Remove(fetchedJob);
                        context.SaveChanges();

                        return new EntityFrameworkFetchedJob(
                            context,
                            transaction,
                            fetchedJob.JobId.ToString(),
                            fetchedJob.Queue);
                    }
                }
                finally
                {
                    if (fetchedJob == null)
                    {
                        transaction?.Dispose();
                        transaction = null;

                        context.Dispose();
                    }
                }

                Thread.Sleep(0);
            } while (true);
        }

        public void Enqueue(string queue, string jobId)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            Guid id = Guid.Parse(jobId);

            Storage.UseHangfireDbContext(context =>
            {
                context.JobQueues.Add(new HangfireJobQueueItem
                {
                    Id = Guid.NewGuid(),
                    JobId = id, Queue = queue,
                    CreatedAt = DateTime.UtcNow
                });
                context.SaveChanges();
            });
        }
    }
}
