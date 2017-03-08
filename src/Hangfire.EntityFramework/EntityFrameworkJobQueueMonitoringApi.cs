﻿// Copyright (c) 2017 Sergey Zhigunov.
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
            if (storage == null) throw new ArgumentNullException(nameof(storage));

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

        public Guid[] GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            queue = queue.ToLowerInvariant();

            return Storage.UseContext(context => (
                from item in context.JobQueues
                where item.Queue == queue
                orderby item.CreatedAt ascending
                select item.JobId).
                Skip(() => from).
                Take(() => perPage).
                ToArray());
        }

        public Guid[] GetFetchedJobIds(string queue, int from, int perPage) => new Guid[0];

        public JobQueueCounters GetJobQueueCounters(string queue)
        {
            queue = queue.ToLowerInvariant();

            long enqueuedCount = Storage.UseContext(context =>
                context.JobQueues.Count(x => x.Queue == queue));

            return new JobQueueCounters { EnqueuedCount = enqueuedCount, };
        }
    }
}