// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using Moq;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class GlobalConfigurationExtensionsTests
    {
        public static object StaticLock { get; } = new object();

        private Mock<IGlobalConfiguration> Configuration { get; }
        private EntityFrameworkJobStorageOptions Options { get; }


        public GlobalConfigurationExtensionsTests()
        {
            Configuration = new Mock<IGlobalConfiguration>();
            Options = new EntityFrameworkJobStorageOptions();
        }

        [Fact]
        public void UseEntityFrameworkJobStorage_ThrowsAnException_WhenConfigurationIsNull()
        {
            IGlobalConfiguration configuration = null;

            Assert.Throws<ArgumentNullException>("configuration",
                () => configuration.UseEntityFrameworkJobStorage(string.Empty));
            Assert.Throws<ArgumentNullException>("configuration",
                () => configuration.UseEntityFrameworkJobStorage(string.Empty, Options));
        }

        [Fact]
        public void UseEntityFrameworkJobStorage_ThrowsAnException_WhenNameOrConnectionStringIsNull()
        {
            Assert.Throws<ArgumentNullException>("nameOrConnectionString",
                () => Configuration.Object.UseEntityFrameworkJobStorage(null));
            Assert.Throws<ArgumentNullException>("nameOrConnectionString",
                () => Configuration.Object.UseEntityFrameworkJobStorage(null, Options));
        }

        [Fact]
        public void UseEntityFrameworkJobStorage_ThrowsAnException_WhenOptionsIsNull()
        {
            Assert.Throws<ArgumentNullException>("options",
                () => Configuration.Object.UseEntityFrameworkJobStorage(string.Empty, null));
        }

        [Fact]
        public void UseEntityFrameworkJobStorage_ConfiguresJobStorageCorrectly()
        {
            string connectionString = Guid.NewGuid().ToString();

            lock (StaticLock)
            {
                Configuration.Object.UseEntityFrameworkJobStorage(connectionString);

                var configuredStorage = JobStorage.Current;
                Assert.NotNull(configuredStorage);
                Assert.IsType<EntityFrameworkJobStorage>(configuredStorage);
                var entityFrameworkJobStorage = (EntityFrameworkJobStorage)configuredStorage;
                Assert.Equal(connectionString, entityFrameworkJobStorage.NameOrConnectionString);
                Assert.NotSame(Options, entityFrameworkJobStorage.Options);

                // Ensure that storage was reconfigured
                Configuration.Object.UseEntityFrameworkJobStorage(connectionString);
                Assert.NotSame(configuredStorage, JobStorage.Current); 
            }
        }

        [Fact]
        public void UseEntityFrameworkJobStorage_ConfiguresJobStorageAndOptionsCorrectly()
        {
            string connectionString = Guid.NewGuid().ToString();
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);

            lock (StaticLock)
            {
                Configuration.Object.UseEntityFrameworkJobStorage(connectionString, Options);

                var configuredStorage = JobStorage.Current;
                Assert.NotNull(configuredStorage);
                Assert.IsType<EntityFrameworkJobStorage>(configuredStorage);
                var entityFrameworkJobStorage = (EntityFrameworkJobStorage)configuredStorage;
                Assert.Equal(connectionString, entityFrameworkJobStorage.NameOrConnectionString);
                Assert.Same(Options, entityFrameworkJobStorage.Options);

                // Ensure that storage was reconfigured
                Configuration.Object.UseEntityFrameworkJobStorage(connectionString, Options);
                Assert.NotSame(configuredStorage, JobStorage.Current);
            }
        }
    }
}
