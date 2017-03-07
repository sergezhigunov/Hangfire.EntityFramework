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
        internal EntityFrameworkJobStorageOptions Options { get; }
        internal string NameOrConnectionString { get; }
        internal virtual PersistentJobQueueProviderCollection QueueProviders { get; }

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
        }

        public override IStorageConnection GetConnection() => new EntityFrameworkJobStorageConnection(this);

        public override IMonitoringApi GetMonitoringApi() => new EntityFrameworkJobStorageMonitoringApi(this);

#pragma warning disable CS0618
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore CS0618
        {
            foreach (var item in base.GetComponents())
                yield return item;

            yield return new CountersAggregator(this, Options.CountersAggregationInterval);
            yield return new ExpirationManager(this, Options.JobExpirationCheckInterval);
        }

        internal void UseHangfireDbContext([InstantHandle] Action<HangfireDbContext> action)
        {
            using (var context = CreateHangfireDbContext())
                action(context);
        }

        internal T UseHangfireDbContext<T>([InstantHandle] Func<HangfireDbContext, T> func)
        {
            T result = default(T);

            UseHangfireDbContext(context =>
            {
                result = func(context);
            });

            return result;
        }

        internal HangfireDbContext CreateHangfireDbContext() => new HangfireDbContext(NameOrConnectionString, Options.DefaultSchemaName);
    }
}