// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using System.Threading;
using Hangfire.EntityFramework.Utils;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    public class EntityFrameworkDistributedLockManagerTests
    {
        private readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkDistributedLockManager(null));
        }

        [Fact]
        public void AcquireDistributedLock_ThrowsAnException_WhenTimeoutIsNegative()
        {
            var manager = CreateManager();
            var negativeTimeout = TimeSpan.FromDays(-1);

            Assert.Throws<ArgumentOutOfRangeException>("timeout",
                () => manager.AcquireDistributedLock("resource", negativeTimeout));
        }

        [Fact]
        public void AcquireDistributedLock_ThrowsAnException_WhenTimeoutTooLargeForLock()
        {
            var manager = CreateManager();
            var tooLargeTimeout = TimeSpan.FromDays(25);

            Assert.Throws<ArgumentOutOfRangeException>("timeout",
                () => manager.AcquireDistributedLock("resource", tooLargeTimeout));
        }

        [Fact]
        public void AcquireDistributedLock_ThrowsAnException_WhenResourceIsNull()
        {
            var manager = CreateManager();

            Assert.Throws<ArgumentNullException>("resource",
                () => manager.AcquireDistributedLock(null, Timeout));
        }

        [Fact]
        public void AcquireDistributedLock_ThrowsAnException_WhenResourceIsEmpty()
        {
            var manager = CreateManager();

            Assert.Throws<ArgumentException>("resource",
                () => manager.AcquireDistributedLock(string.Empty, Timeout));
        }

        [Fact, RollbackTransaction]
        public void AcquireDistributedLock_AcquiresExclusiveApplicationLock()
        {
            var manager = CreateManager();
            string resource = Guid.NewGuid().ToString();

            var start = DateTime.UtcNow.AddSeconds(-1);
            var distributedLock = manager.AcquireDistributedLock(resource, Timeout);
            var end = DateTime.UtcNow.AddSeconds(1);

            var record = UseContext(context => context.DistributedLocks.Single());

            Assert.Equal(resource, record.Id);
            Assert.True(start <= record.CreatedAt && record.CreatedAt <= end);
        }

        [Fact, RollbackTransaction]
        public void AcquireDistributedLock_ThrowsAnException_IfLockCanNotBeGranted()
        {
            string resource = Guid.NewGuid().ToString();
            var timeout = new TimeSpan(0, 0, 2);
            var manager = CreateManager();

            UseContextWithSavingChanges(context =>
                context.DistributedLocks.Add(new HangfireDistributedLock
                {
                    Id = resource,
                    CreatedAt = DateTime.UtcNow,
                }));

            Assert.Throws<DistributedLockTimeoutException>(
                () => manager.AcquireDistributedLock(resource, timeout));

        }

        [Fact]
        public void ReleaseDistributedLock_ThrowsAnException_WhenResourceIsNull()
        {
            var manager = CreateManager();

            Assert.Throws<ArgumentNullException>("resource",
                () => manager.ReleaseDistributedLock(null));
        }

        [Fact]
        public void ReleaseDistributedLock_ThrowsAnException_WhenResourceIsEmpty()
        {
            var manager = CreateManager();

            Assert.Throws<ArgumentException>("resource",
                () => manager.ReleaseDistributedLock(string.Empty));
        }

        [Fact, RollbackTransaction]
        public void ReleaseDistributedLock_ReleasesLock()
        {
            var manager = CreateManager();
            string resource = Guid.NewGuid().ToString();

            UseContextWithSavingChanges(context =>
                context.DistributedLocks.Add(new HangfireDistributedLock
                {
                    Id = resource,
                    CreatedAt = DateTime.UtcNow,
                }));

            manager.ReleaseDistributedLock(resource);

            var record = UseContext(context => context.DistributedLocks.Any());
            Assert.False(record);
        }

        [Fact, RollbackTransaction]
        public void ReleaseDistributedLock_NoThrowsIfLockNotTaken()
        {
            var manager = CreateManager();
            string resource = Guid.NewGuid().ToString();

            manager.ReleaseDistributedLock(resource);

            var record = UseContext(context => context.DistributedLocks.Any());
            Assert.False(record);
        }

        private static EntityFrameworkDistributedLockManager CreateManager() =>
            new EntityFrameworkDistributedLockManager(CreateStorage());
    }
}
