// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity.Infrastructure;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkFetchedJob : IFetchedJob
    {
        private object ThisLock { get; } = new object();

        private EntityFrameworkJobStorage Storage { get; }

        private Guid QueueItemId { get; }

        public string Queue { get; }

        public string JobId { get; }

        private bool Completed { get; set; }

        public EntityFrameworkFetchedJob(
            Guid queueItemId,
            Guid jobId,
            [NotNull] EntityFrameworkJobStorage storage,
            [NotNull] string queue)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            QueueItemId = queueItemId;
            JobId = jobId.ToString();
            Storage = storage;
            Queue = queue;
        }

        public virtual void RemoveFromQueue()
        {
            lock (ThisLock)
                Storage.UseContext(context =>
                {
                    if (!Completed)
                    {
                        var item = context.JobQueues.Attach(new HangfireJobQueueItem { Id = QueueItemId, });
                        context.JobQueues.Remove(item);
                        try
                        {
                            context.SaveChanges();
                        }
                        catch (DbUpdateException)
                        {
                            // Queue item already removed, database wins
                        }
                        Completed = true; 
                    }
                });
        }

        public virtual void Requeue()
        {
            lock (ThisLock)
                Storage.UseContext(context =>
                {
                    if (!Completed)
                    {
                        var item = context.JobQueueLookups.Attach(new HangfireJobQueueItemLookup { QueueItemId = QueueItemId, });
                        context.JobQueueLookups.Remove(item);
                        try
                        {
                            context.SaveChanges();
                        }
                        catch (DbUpdateException)
                        {
                            // Lookup already removed, database wins
                        }
                        Completed = true; 
                    }
                });
        }

        public virtual void Dispose() => Requeue();
    }
}