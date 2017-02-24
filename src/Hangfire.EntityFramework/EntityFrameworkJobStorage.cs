using System;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    /// <summary>
    /// Represents an Entity Framework based job storage.
    /// </summary>
    public class EntityFrameworkJobStorage : JobStorage
    {
        private EntityFrameworkJobStorageOptions Options { get; }
        private string NameOrConnectionString { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="EntityFrameworkJobStorage"/> class with the specified database name
        /// or a connection string.
        /// </summary>
        /// <param name="nameOrConnectionString">
        /// Either the database name or a connection string.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="nameOrConnectionString"/> is <c>null</c>.
        /// </exception>
        public EntityFrameworkJobStorage(string nameOrConnectionString)
            : this(nameOrConnectionString, new EntityFrameworkJobStorageOptions())
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="EntityFrameworkJobStorage"/> class with the specified database name
        /// or a connection string.
        /// </summary>
        /// <param name="nameOrConnectionString">
        /// Either the database name or a connection string.
        /// </param>
        /// <param name="options">
        /// Storage options.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="nameOrConnectionString"/> is <c>null</c>.
        /// -or-
        /// <paramref name="options"/> is <c>null</c>.
        /// </exception>
        public EntityFrameworkJobStorage(string nameOrConnectionString, EntityFrameworkJobStorageOptions options)
        {
            if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));
            if (options == null) throw new ArgumentNullException(nameof(options));

            Options = options;
            NameOrConnectionString = nameOrConnectionString;
        }

        /// <summary>
        /// Creates a new <see cref="IStorageConnection"/> instance for the current storage.
        /// </summary>
        /// <returns>
        /// A new <see cref="IStorageConnection"/> instance.
        /// </returns>
        public override IStorageConnection GetConnection() => new EntityFrameworkJobStorageConnection(this);

        /// <summary>
        /// Creates a new <see cref="IMonitoringApi"/> instance for the current storage.
        /// </summary>
        /// <returns>
        /// A new <see cref="IMonitoringApi"/> instance.
        /// </returns>
        public override IMonitoringApi GetMonitoringApi() => new EntityFrameworkJobStorageMonitoringApi(this);

        internal void UseHangfireDbContext([InstantHandle] Action<HangfireDbContext> action)
        {
            UseHangfireDbContext(context =>
            {
                action(context);
                return true;
            });
        }

        internal T UseHangfireDbContext<T>([InstantHandle] Func<HangfireDbContext, T> func)
        {
            using (var context = CreateHangfireDbContext())
                return func(context);
        }

        private HangfireDbContext CreateHangfireDbContext() => new HangfireDbContext(NameOrConnectionString);
    }
}