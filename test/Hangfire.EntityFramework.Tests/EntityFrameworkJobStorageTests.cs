// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using System.Transactions;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class EntityFrameworkJobStorageTests
    {
        private EntityFrameworkJobStorageOptions Options;

        public EntityFrameworkJobStorageTests()
        {
            Options = new EntityFrameworkJobStorageOptions();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            Assert.Throws<ArgumentNullException>("nameOrConnectionString",
                () => new EntityFrameworkJobStorage(null));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
           Assert.Throws<ArgumentNullException>("options",
                () => new EntityFrameworkJobStorage(string.Empty, null));
        }

        [Fact, CleanDatabase(isolationLevel: IsolationLevel.ReadUncommitted)]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            var api = storage.GetMonitoringApi();
            Assert.NotNull(api);
        }

        [Fact, CleanDatabase]
        public void GetConnection_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            using (var connection = (EntityFrameworkJobStorageConnection)storage.GetConnection())
            {
                Assert.NotNull(connection);
            }
        }

        [Fact, CleanDatabase]
        public void GetComponents_ReturnsCorrectSequence()
        {
            var storage = CreateStorage();

            var components = storage.GetComponents().ToArray();

            Assert.Equal(1, components.Count());
            Assert.True(components.Any(x => x is CountersAggregator));
        }

        private EntityFrameworkJobStorage CreateStorage() =>
            new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString(), Options);
    }
}
