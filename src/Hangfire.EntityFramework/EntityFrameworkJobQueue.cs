﻿// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity.Infrastructure;
using System.Globalization;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobQueue : IPersistentJobQueue
    {
        private static object ThisLock { get; } = new object();

        internal static AutoResetEvent NewItemInQueueEvent { get; } = new AutoResetEvent(true);

        public EntityFrameworkJobStorage Storage { get; }

        public EntityFrameworkJobQueue([NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            Storage = storage;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null)
                throw new ArgumentNullException(nameof(queues));

            if (queues.Length == 0)
                throw new ArgumentException(ErrorStrings.QueuesCannotBeEmpty, nameof(queues));

            queues = queues.Select(x => x.ToUpperInvariant()).ToArray();

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                lock (ThisLock)
                    using (var context = Storage.CreateContext())
                    {
                        var queueItem = (
                            from item in context.JobQueues.
                            WhereContains(x => x.Queue, queues)
                            where item.ServerHostId == null
                            orderby item.Id ascending
                            select item).
                            FirstOrDefault();

                        if (queueItem != null)
                        {
                            queueItem.ServerHostId = EntityFrameworkJobStorage.ServerHostId;
                            try
                            {
                                context.SaveChanges();
                                return new EntityFrameworkFetchedJob(queueItem.Id, queueItem.JobId, Storage, queueItem.Queue);
                            }
                            catch (DbUpdateConcurrencyException)
                            {
                                continue;
                            }
                        }
                    }

                cancellationToken.ThrowIfCancellationRequested();
                WaitHandle.WaitAny(new[] { cancellationToken.WaitHandle, NewItemInQueueEvent }, Storage.Options.QueuePollInterval);
            } while (true);
        }

        public void Enqueue(string queue, string jobId)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));

            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            var id = long.Parse(jobId, CultureInfo.InvariantCulture);
            queue = queue.ToUpperInvariant();

            Storage.UseContext(context =>
            {
                context.JobQueues.Add(new HangfireJobQueue
                {
                    JobId = id,
                    Queue = queue,
                });
                context.SaveChanges();
            });
        }
    }
}
