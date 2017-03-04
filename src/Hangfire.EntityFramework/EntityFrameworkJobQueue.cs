// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobQueue : IPersistentJobQueue
    {
        internal static readonly AutoResetEvent NewItemInQueueEvent = new AutoResetEvent(true);

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

            queues = queues.Select(x => x.ToLowerInvariant()).ToArray();

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
                    {
                        context.JobQueues.Remove(fetchedJob);
                        try
                        {
                            context.SaveChanges();
                        }
                        catch (DbUpdateConcurrencyException)
                        {
                            fetchedJob = null;
                            continue;
                        }

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

                WaitHandle.WaitAny(new[] { cancellationToken.WaitHandle, NewItemInQueueEvent }, Storage.Options.QueuePollInterval);
            } while (true);
        }

        public void Enqueue(string queue, string jobId)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            Guid id = Guid.Parse(jobId);
            queue = queue.ToLowerInvariant();

            Storage.UseHangfireDbContext(context =>
            {
                context.JobQueues.Add(new HangfireJobQueueItem
                {
                    Id = Guid.NewGuid(),
                    JobId = id,
                    Queue = queue,
                    CreatedAt = DateTime.UtcNow
                });
                context.SaveChanges();
            });
        }
    }
}
