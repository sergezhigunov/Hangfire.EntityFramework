using System;

namespace Hangfire.EntityFramework
{
    using static ErrorStrings;

    internal struct TimeoutHelper
    {
        public DateTime Deadline { get; }

        public TimeoutHelper(TimeSpan timeout)
        {
            if (timeout < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(
                    nameof(timeout), timeout,
                    NeedNonNegativeValue);

            if (timeout == TimeSpan.MaxValue)
                Deadline = DateTime.MaxValue;
            else
                Deadline = DateTime.UtcNow + timeout;
        }

        public TimeSpan GetRemainingTime()
        {
            if (Deadline == DateTime.MaxValue)
                return TimeSpan.MaxValue;
            else
            {
                var remaining = Deadline - DateTime.UtcNow;
                return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
            }
        }
    }
}
