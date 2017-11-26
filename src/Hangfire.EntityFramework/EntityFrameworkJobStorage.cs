// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    /// <summary>
    /// Represents an Entity Framework based Hangfire Job Storage.
    /// </summary>
    public class EntityFrameworkJobStorage : JobStorage
    {
        internal static Guid ServerHostId { get; } = Guid.NewGuid();

        private EntityFrameworkJobStorageMonitoringApi MonitoringApi { get; }
        private CountersAggregator CountersAggregator { get; }
        private ExpirationManager ExpirationManager { get; }
        internal EntityFrameworkJobStorageOptions Options { get; }
        internal virtual PersistentJobQueueProviderCollection QueueProviders { get; }
        internal string NameOrConnectionString { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="EntityFrameworkJobStorage"/> class with default options.
        /// </summary>
        /// <param name="nameOrConnectionString">
        /// Either the database name or a connection string.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="nameOrConnectionString"/> parameter is <c>null</c>.
        /// </exception>
        public EntityFrameworkJobStorage(string nameOrConnectionString)
            : this(nameOrConnectionString, new EntityFrameworkJobStorageOptions())
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="EntityFrameworkJobStorage"/> class with the specified options.
        /// </summary>
        /// <param name="nameOrConnectionString">
        /// Either the database name or a connection string.
        /// </param>
        /// <param name="options">
        /// Storage options.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="nameOrConnectionString"/> parameter is <c>null</c>.
        /// -or-
        /// <paramref name="options"/> is <c>null</c>.
        /// </exception>
        public EntityFrameworkJobStorage(string nameOrConnectionString, EntityFrameworkJobStorageOptions options)
        {
            if (nameOrConnectionString == null)
                throw new ArgumentNullException(nameof(nameOrConnectionString));

            if (options == null)
                throw new ArgumentNullException(nameof(options));

            Options = options;
            NameOrConnectionString = nameOrConnectionString;
            var defaultQueueProvider = new EntityFrameworkJobQueueProvider(this);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
            MonitoringApi = new EntityFrameworkJobStorageMonitoringApi(this);
            CountersAggregator = new CountersAggregator(this, options.CountersAggregationInterval);
            ExpirationManager = new ExpirationManager(this, options.JobExpirationCheckInterval);
        }

        /// <summary>
        /// Gets a new instance of storage connection <see cref="EntityFrameworkJobStorageConnection"/>.
        /// </summary>
        /// <returns>
        /// A new <see cref="EntityFrameworkJobStorageConnection"/> instance.
        /// </returns>
        public override IStorageConnection GetConnection() => new EntityFrameworkJobStorageConnection(this);

        /// <summary>
        /// Returns Monitoring API instance.
        /// </summary>
        /// <returns>
        /// An <see cref="IMonitoringApi"/> instance.
        /// </returns>
        public override IMonitoringApi GetMonitoringApi() => MonitoringApi;

        internal void UseContext([InstantHandle] Action<HangfireDbContext> action)
        {
            using (var context = CreateContext())
                action(context);
        }

        internal T UseContext<T>([InstantHandle] Func<HangfireDbContext, T> func)
        {
            T result = default(T);

            UseContext(context =>
            {
                result = func(context);
            });

            return result;
        }

        internal HangfireDbContext CreateContext() =>
            new HangfireDbContext(NameOrConnectionString, Options.DefaultSchemaName);
    }
}