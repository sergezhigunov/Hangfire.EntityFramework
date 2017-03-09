// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobStorage : JobStorage
    {
        internal static Guid ServerHostId { get; } = Guid.NewGuid();

        private EntityFrameworkJobStorageMonitoringApi MonitoringApi { get; }
        private CountersAggregator CountersAggregator { get; }
        private ExpirationManager ExpirationManager { get; }
        internal EntityFrameworkJobStorageOptions Options { get; }
        internal virtual PersistentJobQueueProviderCollection QueueProviders { get; }
        internal string NameOrConnectionString { get; }

        public EntityFrameworkJobStorage(string nameOrConnectionString)
            : this(nameOrConnectionString, new EntityFrameworkJobStorageOptions())
        { }

        public EntityFrameworkJobStorage(string nameOrConnectionString, EntityFrameworkJobStorageOptions options)
        {
            if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));
            if (options == null) throw new ArgumentNullException(nameof(options));

            Options = options;
            NameOrConnectionString = nameOrConnectionString;
            var defaultQueueProvider = new EntityFrameworkJobQueueProvider(this);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
            MonitoringApi = new EntityFrameworkJobStorageMonitoringApi(this);
            CountersAggregator = new CountersAggregator(this, options.CountersAggregationInterval);
            ExpirationManager = new ExpirationManager(this, options.JobExpirationCheckInterval);
        }

        public override IStorageConnection GetConnection() => new EntityFrameworkJobStorageConnection(this);

        public override IMonitoringApi GetMonitoringApi() => new EntityFrameworkJobStorageMonitoringApi(this);

#pragma warning disable CS0618
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore CS0618
        {
            foreach (var item in base.GetComponents())
                yield return item;

            yield return CountersAggregator;
            yield return ExpirationManager;
        }

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

        internal HangfireDbContext CreateContext() => new HangfireDbContext(NameOrConnectionString, Options.DefaultSchemaName);
    }
}