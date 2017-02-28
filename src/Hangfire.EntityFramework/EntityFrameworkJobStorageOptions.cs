using System;

namespace Hangfire.EntityFramework
{
    /// <summary>
    /// Stores options that configure the operation of methods on the <see cref="EntityFrameworkJobStorage"/> class.
    /// </summary>
    public class EntityFrameworkJobStorageOptions
    {
        private TimeSpan _distributedLockTimeout =  new TimeSpan(0, 10, 0);

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
                if (value <= TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException(nameof(value), value, ErrorStrings.NeedPositiveValue);
                _distributedLockTimeout = value;
            }
        }
    }
}