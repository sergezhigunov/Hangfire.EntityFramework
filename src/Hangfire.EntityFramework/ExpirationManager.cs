// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using System.Threading;
using Hangfire.Server;

namespace Hangfire.EntityFramework
{
#pragma warning disable 618
    internal class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private TimeSpan CheckInterval { get; }
        private EntityFrameworkJobStorage Storage { get; }

        public ExpirationManager(EntityFrameworkJobStorage storage, TimeSpan checkInterval)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (checkInterval <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(checkInterval), ErrorStrings.NeedPositiveValue);

            Storage = storage;
            CheckInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            Storage.UseHangfireDbContext(context =>
            {
                var now = DateTime.UtcNow;
                context.Counters.RemoveRange(
                    from counter in context.Counters
                    group counter by counter.Key into @group
                    where @group.Max(x => x.ExpireAt) < now
                    from counter in @group
                    select counter);
                context.Jobs.RemoveRange(context.Jobs.Where(x => x.ExpireAt < now));
                context.Lists.RemoveRange(context.Lists.Where(x => x.ExpireAt < now));
                context.Sets.RemoveRange(context.Sets.Where(x => x.ExpireAt < now));
                context.Hashes.RemoveRange(context.Hashes.Where(x => x.ExpireAt < now));

                context.SaveChanges();
            });

            cancellationToken.WaitHandle.WaitOne(CheckInterval);
        }
    }
}
