// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace Hangfire.EntityFramework
{
    /// <summary>
    /// Stores options that configure the operation of methods on the <see cref="EntityFrameworkJobStorage"/> class.
    /// </summary>
    public class EntityFrameworkJobStorageOptions
    {
        private TimeSpan _distributedLockTimeout = new TimeSpan(0, 10, 0);
        private TimeSpan _queuePollInterval = new TimeSpan(0, 0, 15);
        private TimeSpan _countersAggregationInterval = new TimeSpan(0, 5, 0);
        private TimeSpan _jobExpirationCheckInterval = new TimeSpan(0, 30, 0);
        private string _defaultSchemaName = HangfireConstants.DefaultSchemaName;

        /// <summary>
        /// Gets or set maximal distributed lock lifetime.  The default value is 00:10:00.
        /// </summary>
        /// <value>
        /// A <see cref="TimeSpan"/> value.
        /// </value>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="value"/> is less or equal to <see cref="TimeSpan.Zero"/>.
        /// </exception>
        public TimeSpan DistributedLockTimeout
        {
            get { return _distributedLockTimeout; }
            set
            {
                ThrowIfNonPositive(value);
                _distributedLockTimeout = value;
            }
        }

        /// <summary>
        /// Gets or set queue polling interval. The default value is 00:00:15.
        /// </summary>
        /// <value>
        /// A <see cref="TimeSpan"/> value.
        /// </value>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="value"/> is less or equal to <see cref="TimeSpan.Zero"/>.
        /// </exception>
        public TimeSpan QueuePollInterval
        {
            get { return _queuePollInterval; }
            set
            {
                ThrowIfNonPositive(value);
                _queuePollInterval = value;
            }
        }

        /// <summary>
        /// Gets or set interval between counter aggregation executions. The default value is 00:05:00.
        /// </summary>
        /// <value>
        /// A <see cref="TimeSpan"/> value.
        /// </value>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="value"/> is less or equal to <see cref="TimeSpan.Zero"/>.
        /// </exception>
        public TimeSpan CountersAggregationInterval
        {
            get { return _countersAggregationInterval; }
            set
            {
                ThrowIfNonPositive(value);
                _countersAggregationInterval = value;
            }
        }

        /// <summary>
        /// Gets or set interval between expiration manager executions. The default value is 00:30:00.
        /// </summary>
        /// <value>
        /// A <see cref="TimeSpan"/> value.
        /// </value>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="value"/> is less or equal to <see cref="TimeSpan.Zero"/>.
        /// </exception>
        public TimeSpan JobExpirationCheckInterval
        {
            get { return _jobExpirationCheckInterval; }
            set
            {
                ThrowIfNonPositive(value);
                _jobExpirationCheckInterval = value;
            }
        }

        /// <summary>
        /// Gets or set DB storage schema name.The default value is <c>"Hangfire"</c>
        /// </summary>
        /// <value>
        /// A schema name.
        /// </value>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="value"/> is <c>null</c>.
        /// </exception>
        public string DefaultSchemaName
        {
            get { return _defaultSchemaName; }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(value));
                _defaultSchemaName = value;
            }
        }

        private static void ThrowIfNonPositive(TimeSpan value)
        {
            if (value <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), value, ErrorStrings.NeedPositiveValue);
        }
    }
}