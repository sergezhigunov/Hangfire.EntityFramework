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

    public class EntityFrameworkJobStorageDistributedLockTests
    {
        private readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobStorageDistributedLock(null, "resource", Timeout));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenTimeoutIsNegative()
        {
            var storage = CreateStorage();
            var tooLargeTimeout = TimeSpan.FromDays(-1);

            Assert.Throws<ArgumentOutOfRangeException>("timeout",
                () => new EntityFrameworkJobStorageDistributedLock(storage, "resource", tooLargeTimeout));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenTimeoutTooLargeForLock()
        {
            var storage = CreateStorage();
            var tooLargeTimeout = TimeSpan.FromDays(25);

            Assert.Throws<ArgumentOutOfRangeException>("timeout",
                () => new EntityFrameworkJobStorageDistributedLock(storage, "resource", tooLargeTimeout));
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            var storage = CreateStorage();

            Assert.Throws<ArgumentNullException>("resource",
                () => new EntityFrameworkJobStorageDistributedLock(storage, null, Timeout));
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenResourceIsEmpty()
        {
            var storage = CreateStorage();

            Assert.Throws<ArgumentException>("resource",
                () => new EntityFrameworkJobStorageDistributedLock(storage, string.Empty, Timeout));
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock()
        {
            string resource = Guid.NewGuid().ToString();
            var storage = CreateStorage();

            var start = DateTime.UtcNow.AddSeconds(-1);
            var distributedLock = new EntityFrameworkJobStorageDistributedLock(storage, resource, Timeout);
            var end = DateTime.UtcNow.AddSeconds(1);

            var record = UseContext(context => context.DistributedLocks.Single());

            Assert.Equal(resource, record.Id);
            Assert.True(start <= record.CreatedAt && record.CreatedAt <= end);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted()
        {
            string resource = Guid.NewGuid().ToString();
            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);

            var thread = new Thread(
                () =>
                {
                    var storage = CreateStorage();
                    using (new EntityFrameworkJobStorageDistributedLock(storage, resource, Timeout))
                    {
                        lockAcquired.Set();
                        releaseLock.Wait();
                    }
                });
            thread.Start();

            lockAcquired.Wait();

            {
                var storage = CreateStorage();
                Assert.Throws<EntityFrameworkDistributedLockTimeoutException>(
                    () => new EntityFrameworkJobStorageDistributedLock(storage, resource, Timeout));
            }

            releaseLock.Set();
            thread.Join();
        }

        [Fact, CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock()
        {
            string resource = Guid.NewGuid().ToString();
            var storage = CreateStorage();

            var distributedLock = new EntityFrameworkJobStorageDistributedLock(storage, resource, Timeout);
            distributedLock.Dispose();

            var record = UseContext(context => context.DistributedLocks.Any());
            Assert.False(record);
        }
    }
}
