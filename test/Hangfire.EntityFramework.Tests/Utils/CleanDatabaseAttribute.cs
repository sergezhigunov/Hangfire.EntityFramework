// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Reflection;
using Xunit.Sdk;

namespace Hangfire.EntityFramework.Utils
{
    using System.Data.Entity;
    using static ConnectionUtils;

    [AttributeUsage(AttributeTargets.Class)]
    internal sealed class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static volatile bool _cleaned;

        private static object StaticLock { get; } = new object();

        public override void Before(MethodInfo methodUnderTest)
        {
            if (!_cleaned)
                lock (StaticLock)
                    if (!_cleaned)
                    {
                        CleanDatabase();
                        _cleaned = true;
                    }
        }

        private static void CleanDatabase() =>
            UseContextWithSavingChanges(context =>
            {
                CleanDbSet(context.Counters);
                CleanDbSet(context.DistributedLocks);
                CleanDbSet(context.Hashes);
                CleanDbSet(context.JobParameters);
                CleanDbSet(context.JobQueues);
                CleanDbSet(context.JobStates);
                CleanDbSet(context.Jobs);
                CleanDbSet(context.Lists);
                CleanDbSet(context.Servers);
                CleanDbSet(context.ServerHosts);
                CleanDbSet(context.Sets);
            });

        private static void CleanDbSet<T>(DbSet<T> dbSet)
            where T : class
        {
            dbSet.RemoveRange(dbSet);
        }
    }
}
