// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace Hangfire.EntityFramework.Utils
{
    internal static class ConnectionUtils
    {
        internal static string GetConnectionString() => "Hangfire.EntityFramework.Tests";

        internal static EntityFrameworkJobStorage CreateStorage()
        {
            string connectionString = GetConnectionString();
            return new EntityFrameworkJobStorage(connectionString);
        }

        internal static HangfireDbContext CreateContext()
        {
            var storage = CreateStorage();
            return storage.CreateContext();
        }

        internal static void UseContext(Action<HangfireDbContext> action)
        {
            var storage = CreateStorage();
            storage.UseContext(action);
        }

        internal static T UseContext<T>(Func<HangfireDbContext, T> func)
        {
            T result = default(T);
            UseContext(context => { result = func(context); });
            return result;
        }

        internal static void UseContextWithSavingChanges(Action<HangfireDbContext> action)
        {
            var storage = CreateStorage();
            storage.UseContext(context =>
            {
                action(context);
                context.SaveChanges();
            });
        }

        internal static T UseContextWithSavingChanges<T>(Func<HangfireDbContext, T> func)
        {
            T result = default(T);
            UseContextWithSavingChanges(context => { result = func(context); });
            return result;
        }

        internal static void UseConnection(Action<EntityFrameworkJobStorageConnection> action)
        {
            var storage = CreateStorage();

            using (var connection = new EntityFrameworkJobStorageConnection(storage))
                action(connection);
        }

        internal static T UseConnection<T>(Func<EntityFrameworkJobStorageConnection, T> func)
        {
            T result = default(T);
            UseConnection(connection => { result = func(connection); });
            return result;
        }

        internal static EntityFrameworkJobStorageTransaction CreateTransaction()
        {
            var storage = CreateStorage();

            return new EntityFrameworkJobStorageTransaction(storage);
        }

        internal static void UseTransaction(Action<EntityFrameworkJobStorageTransaction> action)
        {
            var storage = CreateStorage();

            using (var transaction = CreateTransaction())
            {
                action(transaction);
                transaction.Commit();
            }
        }
    }
}