// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using Hangfire.EntityFramework.Utils;
using Moq;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    public class EntityFrameworkDistributedLockTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenManagerIsNull()
        {
            Assert.Throws<ArgumentNullException>("manager",
                () => new EntityFrameworkDistributedLock(null, "resource"));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            var manager = CreateManager();

            Assert.Throws<ArgumentNullException>("resource",
                () => new EntityFrameworkDistributedLock(manager.Object, null));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsEmpty()
        {
            var manager = CreateManager();

            Assert.Throws<ArgumentException>("resource",
                () => new EntityFrameworkDistributedLock(manager.Object, string.Empty));
        }

        [Fact]
        public void Dispose_CallsReleaseDistributedLock()
        {
            var manager = CreateManager();
            var resource = "resource";

            using (var distributedLock = new EntityFrameworkDistributedLock(manager.Object, resource))
                manager.Verify(x => x.ReleaseDistributedLock(resource), Times.Never);

            manager.Verify(x => x.ReleaseDistributedLock(resource), Times.Once);
        }

        private static Mock<EntityFrameworkDistributedLockManager> CreateManager() =>
            new Mock<EntityFrameworkDistributedLockManager>(CreateStorage());
    }
}
