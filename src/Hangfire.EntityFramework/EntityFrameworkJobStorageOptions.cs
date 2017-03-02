using System;

namespace Hangfire.EntityFramework
{
    /// <summary>
    /// Stores options that configure the operation of methods on the <see cref="EntityFrameworkJobStorage"/> class.
    /// </summary>
    public class EntityFrameworkJobStorageOptions
    {
        private TimeSpan _distributedLockTimeout = new TimeSpan(0, 10, 0);
        private TimeSpan _queuePollInterval = new TimeSpan(0, 0, 5);

        /// <summary>
        /// Gets or set maximal distributed lock lifetime.
        /// </summary>
        /// <value>
        /// A <see cref="TimeSpan"/> value.
        /// </value>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="value"/> is less or equal to <see cref="TimeSpan.Zero"/>
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
        /// Gets or set queue polling interval.
        /// </summary>
        /// <value>
        /// A <see cref="TimeSpan"/> value.
        /// </value>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="value"/> is less or equal to <see cref="TimeSpan.Zero"/>
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
        /// Gets or set DB storage schema name.
        /// </summary>
        /// <value>
        /// A schema name.
        /// </value>
        public string DefaultSchemaName { get; set; }

        private static void ThrowIfNonPositive(TimeSpan value)
        {
            if (value <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), value, ErrorStrings.NeedPositiveValue);
        }
    }
}