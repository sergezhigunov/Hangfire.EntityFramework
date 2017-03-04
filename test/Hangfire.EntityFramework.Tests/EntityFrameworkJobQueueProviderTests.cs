// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class EntityFrameworkJobQueueProviderTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobQueueProvider(null));
        }

        [Fact]
        public void Ctor_EnsureDefaults()
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());

            var result = new EntityFrameworkJobQueueProvider(storage);

            Assert.NotNull(result.GetJobQueue());
            Assert.NotNull(result.GetJobQueueMonitoringApi());
        }

        [Fact]
        public void GetJobQueue_ReturnsCorrectType()
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            var provider = new EntityFrameworkJobQueueProvider(storage);

            var result = provider.GetJobQueue();

            Assert.IsType<EntityFrameworkJobQueue>(result);
        }

        [Fact]
        public void GetJobQueueMonitoringApi_ReturnsCorrectType()
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            var provider = new EntityFrameworkJobQueueProvider(storage);

            var result = provider.GetJobQueueMonitoringApi();

            Assert.IsType<EntityFrameworkJobQueueMonitoringApi>(result);
        }
    }
}