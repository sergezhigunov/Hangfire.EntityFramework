// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Storage;

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
                using (var context = Storage.CreateContext())
                {
                    context.DistributedLocks.Add(new HangfireDistributedLock
                    {
                        Id = resource,
                        CreatedAt = DateTime.UtcNow,
                    });

                    try
                    {
                        context.SaveChanges();
                        return; // Lock taken
                    }
                    catch (DbUpdateException)
                    {
                        // Lock already exists
                    }
                }

                using (var context = Storage.CreateContext())
                {
                    var distributedLock = context.DistributedLocks.SingleOrDefault(x => x.Id == resource);

                    // If the lock has been removed
                    if (distributedLock == null) 
                        continue; // We should try to insert again

                    DateTime expireAt = distributedLock.CreatedAt + Storage.Options.DistributedLockTimeout;

                    // If the lock has been expired, we should delete it
                    if (expireAt < DateTime.UtcNow)
                    {
                        context.DistributedLocks.Remove(distributedLock);

                        try
                        {
                            context.SaveChanges();
                        }
                        catch (DbUpdateConcurrencyException)
                        {
                            // Already deleted, database wins
                        }

                        continue; // We should try to insert again
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

            throw new DistributedLockTimeoutException(resource);
        }

        public virtual void ReleaseDistributedLock(string resource)
        {
            ValidateResource(resource);

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

            if (resource.Length == 0)
                throw new ArgumentException(ErrorStrings.StringCannotBeEmpty, nameof(resource));
        }
    }
}
