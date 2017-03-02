using System;
using System.Collections.Generic;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class EntityFrameworkJobStorageOptionsTests
    {
        [Fact]
        public void Ctor_EnsureDefaults()
        {
            var options = new EntityFrameworkJobStorageOptions();

            Assert.Equal("Hangfire", options.DefaultSchemaName);
            Assert.Equal(new TimeSpan(0, 10, 0), options.DistributedLockTimeout);
            Assert.Equal(new TimeSpan(0, 0, 5), options.QueuePollInterval);
        }

        [Fact]
        public void DistributedLockTimeout_SetChangesValue()
        {
            var valueToSet = new TimeSpan(1, 2, 3);
            var options = new EntityFrameworkJobStorageOptions();

            options.DistributedLockTimeout = valueToSet;

            Assert.Equal(valueToSet, options.DistributedLockTimeout);
        }

        [Theory]
        [MemberData(nameof(GetNonPositiveTimestamps))]
        public void DistributedLockTimeout_ThrowsAnException_WhenValueNonPositive(TimeSpan value)
        {
            var options = new EntityFrameworkJobStorageOptions();

            Assert.Throws<ArgumentOutOfRangeException>("value", () => options.DistributedLockTimeout = value);
        }

        [Fact]
        public void QueuePollInterval_SetChangesValue()
        {
            var valueToSet = new TimeSpan(1, 2, 3);
            var options = new EntityFrameworkJobStorageOptions();

            options.QueuePollInterval = valueToSet;

            Assert.Equal(valueToSet, options.QueuePollInterval);
        }

        [Theory]
        [MemberData(nameof(GetNonPositiveTimestamps))]
        public void QueuePollInterval_ThrowsAnException_WhenValueNonPositive(TimeSpan value)
        {
            var options = new EntityFrameworkJobStorageOptions();

            Assert.Throws<ArgumentOutOfRangeException>("value", () => options.QueuePollInterval = value);
        }

        private static IEnumerable<object[]> GetNonPositiveTimestamps()
        {
            yield return new object[] { TimeSpan.MinValue, };
            yield return new object[] { new TimeSpan(-1), };
            yield return new object[] { TimeSpan.Zero, };
        }
    }
}
