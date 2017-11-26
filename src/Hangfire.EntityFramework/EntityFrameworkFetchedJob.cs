// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Globalization;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkFetchedJob : IFetchedJob
    {
        private object ThisLock { get; } = new object();

        private EntityFrameworkJobStorage Storage { get; }

        private long QueueItemId { get; }

        public string Queue { get; }

        public long JobId { get; }

        private bool Completed { get; set; }

        private bool Disposed { get; set; }

        string IFetchedJob.JobId => JobId.ToString(CultureInfo.InvariantCulture);

        public EntityFrameworkFetchedJob(
            long queueItemId,
            long jobId,
            [NotNull] EntityFrameworkJobStorage storage,
            [NotNull] string queue)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));

            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            QueueItemId = queueItemId;
            JobId = jobId;
            Storage = storage;
            Queue = queue;
        }

        public virtual void RemoveFromQueue()
        {
            if (!Completed)
                lock (ThisLock)
                    if (!Completed)
                    {
                        Storage.UseContext(context =>
                        {
                            // Remove item from queue
                            RemoveQueueItem(context, QueueItemId);

                            try
                            {
                                context.SaveChanges();
                            }
                            catch (DbUpdateConcurrencyException)
                            {
                                // Queue item already removed, database wins
                            }
                        });

                        Completed = true;
                    }
        }

        public virtual void Requeue()
        {
            if (!Completed)
                lock (ThisLock)
                    if (!Completed)
                    {
                        Storage.UseContext(context =>
                        {
                            using (var transaction = context.Database.BeginTransaction())
                            {
                                // Add item to the end of the queue
                                context.JobQueues.Add(new HangfireJobQueueItem
                                {
                                    JobId = JobId,
                                    Queue = Queue,
                                });

                                context.SaveChanges();

                                // Remove item from the start of the queue
                                RemoveQueueItem(context, QueueItemId);

                                try
                                {
                                    context.SaveChanges();
                                }
                                catch (DbUpdateConcurrencyException)
                                {
                                    // Queue item already removed, database wins
                                }

                                transaction.Commit();
                            }
                        });

                        Completed = true;
                    }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!Disposed)
            {
                if (disposing)
                {
                    Requeue();
                    Disposed = true;
                }
            }
        }

        private static void RemoveQueueItem(HangfireDbContext context, long itemId)
        {
            context.
                Entry(new HangfireJobQueueItem{ Id = itemId, }).
                State = EntityState.Deleted;
        }
    }
}