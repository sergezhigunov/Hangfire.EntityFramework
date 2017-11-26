// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity.Infrastructure;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Server;

namespace Hangfire.EntityFramework
{
    internal class CountersAggregator : IBackgroundProcess
    {
        private EntityFrameworkJobStorage Storage { get; }
        private TimeSpan AggregationInterval { get; }

        public CountersAggregator(EntityFrameworkJobStorage storage, TimeSpan aggregationInterval)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            if (aggregationInterval <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(aggregationInterval), ErrorStrings.NeedPositiveValue);

            Storage = storage;
            AggregationInterval = aggregationInterval;
        }

        public void Execute([NotNull] BackgroundProcessContext context)
        {
            int removedCount;
            bool concurrencyExceptionThrown;
            var cancellationToken = context.CancellationToken;
            do
            {
                removedCount = 0;
                concurrencyExceptionThrown = false;
                Storage.UseContext(dbContext =>
                {
                    var key = (
                        from counter in dbContext.Counters
                        group counter by counter.Key into @group
                        let count = @group.Count()
                        where count > 1
                        orderby count descending
                        select @group.Key).
                        FirstOrDefault();

                    if (key != null)
                    {
                        var itemsToRemove = (
                            from counter in dbContext.Counters
                            where counter.Key == key
                            select counter).
                            Take(1000).
                            ToArray();

                        if (itemsToRemove.Length > 1)
                        {
                            dbContext.Counters.RemoveRange(itemsToRemove);
                            dbContext.Counters.Add(new HangfireCounter
                            {
                                Key = key,
                                Value = itemsToRemove.Sum(x => x.Value),
                                ExpireAt = itemsToRemove.Max(x => x.ExpireAt),
                            });
                            removedCount += itemsToRemove.Length;

                            try
                            {
                                dbContext.SaveChanges();
                            }
                            catch (DbUpdateConcurrencyException)
                            {
                                concurrencyExceptionThrown = true;
                            } 
                        }
                    }
                });

                if (concurrencyExceptionThrown || removedCount > 0)
                {
                    cancellationToken.WaitHandle.WaitOne(new TimeSpan(0, 0, 1));
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (concurrencyExceptionThrown || removedCount > 0);

            cancellationToken.WaitHandle.WaitOne(AggregationInterval);
        }
    }
}
