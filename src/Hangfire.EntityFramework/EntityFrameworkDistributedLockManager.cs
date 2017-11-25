// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkDistributedLockManager
    {
        private static TimeSpan MaxSupportedTimeout { get; } =
            new TimeSpan(TimeSpan.TicksPerMillisecond * int.MaxValue);

        private static TimeSpan MaxThreadSleepTimeout { get; } =
            new TimeSpan(TimeSpan.TicksPerSecond);

        private EntityFrameworkJobStorage Storage { get; }

        public EntityFrameworkDistributedLockManager(
            [NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            Storage = storage;
        }

        public virtual EntityFrameworkDistributedLock AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            ValidateResource(resource);

            if (timeout < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(
                    nameof(timeout),
                    timeout,
                    ErrorStrings.NeedNonNegativeValue);

            if (timeout > MaxSupportedTimeout)
                throw new ArgumentOutOfRangeException(
                    nameof(timeout),
                    timeout,
                    string.Format(
                        ErrorStrings.Culture,
                        ErrorStrings.TimeoutTooLarge,
                        MaxSupportedTimeout));

            Initialize(resource, timeout);

            return new EntityFrameworkDistributedLock(this, resource);
        }

        private void Initialize(string resource, TimeSpan timeout)
        {
            var timeoutHelper = new TimeoutHelper(timeout);

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                TryRemoveDeadlock(resource);

                using (var context = Storage.CreateContext())
                using (var transaction = context.Database.BeginTransaction())
                {
                    if (!context.DistributedLocks.Any(x => x.Id == resource))
                    {
                        context.DistributedLocks.Add(new HangfireDistributedLock
                        {
                            Id = resource,
                            CreatedAt = DateTime.UtcNow,
                        });

                        bool alreadyLocked = false;
                        try
                        {
                            context.SaveChanges();
                        }
                        catch (DbUpdateException)
                        {
                            alreadyLocked = true;
                        }
                        finally
                        {
                            transaction.Commit();
                        }

                        if (!alreadyLocked)
                            return;
                    }
                }

                var remainingTime = timeoutHelper.GetRemainingTime();

                if (remainingTime == TimeSpan.Zero)
                    tryAcquireLock = false;
                else
                {
                    var sleepDuration =
                        remainingTime > MaxThreadSleepTimeout ?
                        MaxThreadSleepTimeout :
                        remainingTime;

                    Thread.Sleep(sleepDuration);
                }
            }

            throw new EntityFrameworkDistributedLockTimeoutException(
                string.Format(ErrorStrings.Culture, ErrorStrings.LockTimedOutOnResource, resource));
        }

        private void TryRemoveDeadlock(string resource)
        {
            ValidateResource(resource);

            Storage.UseContext(context =>
            {
                using (var transaction = context.Database.BeginTransaction())
                {
                    DateTime distributedLockExpiration = DateTime.UtcNow - Storage.Options.DistributedLockTimeout;

                    if (context.DistributedLocks.Any(x => x.Id == resource && x.CreatedAt < distributedLockExpiration))
                    {
                        context.Entry(new HangfireDistributedLock { Id = resource }).State = EntityState.Deleted;
                        context.SaveChanges();
                    }

                    transaction.Commit();
                }
            });
        }

        public virtual void ReleaseDistributedLock(string resource)
        {
            if (resource == null)
                throw new ArgumentNullException(nameof(resource));

            if (resource == string.Empty)
                throw new ArgumentException(ErrorStrings.StringCannotBeEmpty, nameof(resource));

            Storage.UseContext(context =>
            {
                context.Entry(new HangfireDistributedLock { Id = resource }).State = EntityState.Deleted;
                try
                {
                    context.SaveChanges();
                }
                catch (DbUpdateConcurrencyException)
                {
                    // Database wins
                }
            });
        }

        private static void ValidateResource(string resource)
        {
            if (resource == null)
                throw new ArgumentNullException(nameof(resource));

            if (resource == string.Empty)
                throw new ArgumentException(ErrorStrings.StringCannotBeEmpty, nameof(resource));
        }
    }
}
