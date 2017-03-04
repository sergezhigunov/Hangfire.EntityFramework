// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkFetchedJob : IFetchedJob
    {
        private object ThisLock { get; } = new object();

        private HangfireDbContext Context { get; }

        private DbContextTransaction Transaction { get; }

        public string Queue { get; }

        public string JobId { get; }

        private bool Disposed { get; set; }

        public EntityFrameworkFetchedJob(
            [NotNull] HangfireDbContext context,
            [NotNull] DbContextTransaction transaction,
            string jobId,
            string queue)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            if (transaction == null) throw new ArgumentNullException(nameof(transaction));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            Context = context;
            Transaction = transaction;
            JobId = jobId;
            Queue = queue;
        }

        public void RemoveFromQueue()
        {
            lock (ThisLock)
                Transaction.Commit();
        }

        public void Requeue()
        {
            lock (ThisLock)
                Transaction.Rollback();
        }

        public void Dispose()
        {
            if (!Disposed)
            lock (ThisLock)
                if (!Disposed)
                {
                    Transaction.Dispose();
                    Context.Dispose();
                    Disposed = true;
                }
        }
    }
}