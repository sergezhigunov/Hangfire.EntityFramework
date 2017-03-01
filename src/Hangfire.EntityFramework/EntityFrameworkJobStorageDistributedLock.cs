using System;
using System.Data;
using System.Data.Entity;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobStorageDistributedLock : IDisposable
    {
        private static TimeSpan MaxSupportedTimeout = new TimeSpan(TimeSpan.TicksPerMillisecond * int.MaxValue);

        private EntityFrameworkJobStorage Storage { get; }
        private string Resource { get; }
        private TimeSpan Timeout { get; }
        private bool Disposed { get; set; }

        public EntityFrameworkJobStorageDistributedLock(
            EntityFrameworkJobStorage storage,
            string resource,
            TimeSpan timeout)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            if (resource == string.Empty)
                throw new ArgumentException(ErrorStrings.StringCannotBeEmpty, nameof(resource));
            if (timeout < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(timeout), timeout, ErrorStrings.NeedNonNegativeValue);
            if (timeout > MaxSupportedTimeout) throw new ArgumentOutOfRangeException(nameof(timeout), timeout,
                string.Format(ErrorStrings.Culture, ErrorStrings.TimeoutTooLarge, MaxSupportedTimeout));

            Resource = resource;
            Timeout = timeout;
            Storage = storage;

            Initialize();
        }

        private void Initialize()
        {
            var lockAcquiringTime = Stopwatch.StartNew();

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                TryRemoveDeadlock();

                using (var context = Storage.CreateHangfireDbContext())
                using (var transaction = context.Database.BeginTransaction())
                {
                    if (!context.DistributedLocks.Any(x => x.Resource == Resource))
                    {
                        context.DistributedLocks.Add(new HangfireDistributedLock { Resource = Resource, CreatedAt = DateTime.UtcNow, });
                        context.SaveChanges();
                        transaction.Commit();
                        return;
                    }
                    transaction.Commit();
                }

                if (lockAcquiringTime.ElapsedMilliseconds > Timeout.TotalMilliseconds)
                    tryAcquireLock = false;
                else
                {
                    int sleepDuration = Math.Min(1000, (int)(Timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds));
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                        Thread.Sleep(sleepDuration);
                    else
                        tryAcquireLock = false;
                }
            }

            throw new EntityFrameworkDistributedLockTimeoutException(
                string.Format(ErrorStrings.Culture, ErrorStrings.LockTimedOutOnResource, Resource));
        }

        private void TryRemoveDeadlock()
        {
            Storage.UseHangfireDbContext(context =>
            {
                using (var transaction = context.Database.BeginTransaction())
                {
                    DateTime distributedLockExpiration = DateTime.UtcNow - Storage.Options.DistributedLockTimeout;

                    if (context.DistributedLocks.Any(x => x.Resource == Resource && x.CreatedAt < distributedLockExpiration))
                    {
                        context.Entry(new HangfireDistributedLock { Resource = Resource }).State = EntityState.Deleted;
                        context.SaveChanges();
                    }

                    transaction.Commit();
                }
            });
        }

        public void Dispose()
        {
            if (!Disposed)
            {
                Storage.UseHangfireDbContext(context =>
                {
                    using (var transaction = context.Database.BeginTransaction())
                    {
                        if (context.DistributedLocks.Any(x => x.Resource == Resource))
                        {
                            context.Entry(new HangfireDistributedLock { Resource = Resource }).State = EntityState.Deleted;
                            context.SaveChanges();
                        }
                        transaction.Commit();
                    }
                });
                Disposed = true;
            }
        }
    }
}