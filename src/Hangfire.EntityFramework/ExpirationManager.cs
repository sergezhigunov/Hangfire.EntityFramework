// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Server;

namespace Hangfire.EntityFramework
{
    internal class ExpirationManager : IBackgroundProcess
    {
        private TimeSpan CheckInterval { get; }
        private EntityFrameworkJobStorage Storage { get; }

        public ExpirationManager(EntityFrameworkJobStorage storage, TimeSpan checkInterval)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            if (checkInterval <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(checkInterval), ErrorStrings.NeedPositiveValue);

            Storage = storage;
            CheckInterval = checkInterval;
        }

        public void Execute([NotNull] BackgroundProcessContext context)
        {
            var cancellationToken = context.CancellationToken;

            Storage.UseContext(dbContext =>
            {
                var now = DateTime.UtcNow;
                dbContext.Counters.RemoveRange(
                    from counter in dbContext.Counters
                    group counter by counter.Key into @group
                    where @group.Max(x => x.ExpireAt) < now
                    from counter in @group
                    select counter);

                dbContext.Jobs.
                    RemoveRange(dbContext.Jobs.Where(x => x.ExpireAt < now));

                dbContext.Lists.
                    RemoveRange(dbContext.Lists.Where(x => x.ExpireAt < now));

                dbContext.Sets.
                    RemoveRange(dbContext.Sets.Where(x => x.ExpireAt < now));

                dbContext.Hashes.
                    RemoveRange(dbContext.Hashes.Where(x => x.ExpireAt < now));

                dbContext.SaveChanges();
            });

            cancellationToken.WaitHandle.WaitOne(CheckInterval);
        }
    }
}
