// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using Hangfire.Annotations;

namespace Hangfire.EntityFramework
{
    /// <summary>
    /// Represents extensions to configure Entity Framework storage for Hangfire.
    /// </summary>
    public static class GlobalConfigurationExtensions
    {
        /// <summary>
        /// Configure Hangfire to use Entity Framework storage with the default options.
        /// </summary>
        /// <param name="configuration">
        /// Hangfire configuration <see cref="IGlobalConfiguration"/>.
        /// </param>
        /// <param name="nameOrConnectionString">
        /// Either the database name or a connection string.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="nameOrConnectionString"/> is <c>null</c>.
        /// </exception>
        public static void UseEntityFrameworkJobStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] string nameOrConnectionString)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));

            var storage = new EntityFrameworkJobStorage(nameOrConnectionString);
            configuration.UseStorage(storage);
        }

        /// <summary>
        /// Configure Hangfire to use Entity Framework storage with the specified options.
        /// </summary>
        /// <param name="configuration">
        /// Hangfire configuration <see cref="IGlobalConfiguration"/>.
        /// </param>
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
        public static void UseEntityFrameworkJobStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] string nameOrConnectionString,
            [NotNull] EntityFrameworkJobStorageOptions options)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (options == null) throw new ArgumentNullException(nameof(options));

            var storage = new EntityFrameworkJobStorage(nameOrConnectionString, options);
            configuration.UseStorage(storage);
        }
    }
}
