// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using System.Threading;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    public class ExpirationManagerTests
    {
        private TimeSpan CheckInterval { get; } = new TimeSpan(1);
        private CancellationTokenSource CancellationTokenSource { get; } = new CancellationTokenSource();

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new ExpirationManager(null, CheckInterval));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsNonPositive(int interval)
        {
            var storage = CreateStorage();

            Assert.Throws<ArgumentOutOfRangeException>("checkInterval",
                () => new ExpirationManager(storage, new TimeSpan(0, 0, interval)));
        }

        [Fact, RollbackTransaction]
        public void Execute_RemovesOutdatedRecords()
        {
            var storage = CreateStorage();
            CreateExpirationEntries(DateTime.UtcNow.AddMonths(-1));
            var manager = new ExpirationManager(storage, CheckInterval);

            manager.Execute(CancellationTokenSource.Token);

            UseContext(context =>
            {
                Assert.False(context.Counters.Any());
                Assert.False(context.Jobs.Any());
                Assert.False(context.Lists.Any());
                Assert.False(context.Sets.Any());
                Assert.False(context.Hashes.Any());
            });
        }

        [Fact, RollbackTransaction]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            var storage = CreateStorage();
            CreateExpirationEntries(null);
            var manager = new ExpirationManager(storage, CheckInterval);

            manager.Execute(CancellationTokenSource.Token);

            UseContext(context =>
            {
                Assert.Equal(1, context.Counters.Count());
                Assert.Equal(1, context.Jobs.Count());
                Assert.Equal(1, context.Lists.Count());
                Assert.Equal(1, context.Sets.Count());
                Assert.Equal(1, context.Hashes.Count());
            });
        }

        [Fact, RollbackTransaction]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            var storage = CreateStorage();
            CreateExpirationEntries(DateTime.UtcNow.AddMonths(1));
            var manager = new ExpirationManager(storage, CheckInterval);

            manager.Execute(CancellationTokenSource.Token);

            UseContext(context =>
            {
                Assert.Equal(1, context.Counters.Count());
                Assert.Equal(1, context.Jobs.Count());
                Assert.Equal(1, context.Lists.Count());
                Assert.Equal(1, context.Sets.Count());
                Assert.Equal(1, context.Hashes.Count());
            });
        }

        private void CreateExpirationEntries(DateTime? expireAt)
        {
            UseContextWithSavingChanges(context =>
            {
                context.Counters.Add(new HangfireCounter
                {
                    Id = Guid.NewGuid(),
                    Key = "test",
                    ExpireAt = expireAt,
                });

                context.Jobs.Add(new HangfireJob
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = DateTime.UtcNow,
                    ExpireAt = expireAt,
                });

                context.Lists.Add(new HangfireListItem
                {
                    Key = "test",
                    ExpireAt = expireAt,
                });

                context.Sets.Add(new HangfireSet
                {
                    Key = "test",
                    Value = "test",
                    CreatedAt = DateTime.UtcNow,
                    ExpireAt = expireAt,
                });

                context.Hashes.Add(new HangfireHash
                {
                    Key = "test",
                    Field = "test",
                    ExpireAt = expireAt,
                });
            });
        }
    }
}
