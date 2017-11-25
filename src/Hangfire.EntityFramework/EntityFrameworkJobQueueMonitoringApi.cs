// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

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
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            Storage = storage;
        }

        public string[] GetQueues()
        {
            return Storage.UseContext(context => (
                from queueItem in context.JobQueues
                select queueItem.Queue).
                Distinct().
                ToArray());
        }

        public long[] GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            queue = queue.ToUpperInvariant();

            return Storage.UseContext(context => (
                from item in context.JobQueues
                where item.Queue == queue && item.Lookup == null
                orderby item.CreatedAt ascending
                select item.JobId).
                Skip(() => from).
                Take(() => perPage).
                ToArray());
        }

        public long GetEnqueuedJobCount(string queue)
        {
            queue = queue.ToUpperInvariant();

            return Storage.UseContext(context =>
                context.JobQueues.Count(x => x.Queue == queue && x.Lookup == null));
        }
    }
}