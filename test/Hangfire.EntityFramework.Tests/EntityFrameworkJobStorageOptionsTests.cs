// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

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
            Assert.Equal(new TimeSpan(0, 0, 15), options.QueuePollInterval);
            Assert.Equal(new TimeSpan(0, 5, 0), options.CountersAggregationInterval);
            Assert.Equal(new TimeSpan(0, 30, 0), options.JobExpirationCheckInterval);
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

            Assert.Throws<ArgumentOutOfRangeException>("value",
                () => options.DistributedLockTimeout = value);
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

            Assert.Throws<ArgumentOutOfRangeException>("value",
                () => options.QueuePollInterval = value);
        }

        [Fact]
        public void CountersAggregationInterval_SetChangesValue()
        {
            var valueToSet = new TimeSpan(1, 2, 3);
            var options = new EntityFrameworkJobStorageOptions();

            options.CountersAggregationInterval = valueToSet;

            Assert.Equal(valueToSet, options.CountersAggregationInterval);
        }

        [Theory]
        [MemberData(nameof(GetNonPositiveTimestamps))]
        public void CountersAggregationInterval_ThrowsAnException_WhenValueNonPositive(TimeSpan value)
        {
            var options = new EntityFrameworkJobStorageOptions();

            Assert.Throws<ArgumentOutOfRangeException>("value",
                () => options.CountersAggregationInterval = value);
        }

        [Fact]
        public void JobExpirationCheckInterval_SetChangesValue()
        {
            var valueToSet = new TimeSpan(1, 2, 3);
            var options = new EntityFrameworkJobStorageOptions();

            options.JobExpirationCheckInterval = valueToSet;

            Assert.Equal(valueToSet, options.JobExpirationCheckInterval);
        }

        [Theory]
        [MemberData(nameof(GetNonPositiveTimestamps))]
        public void JobExpirationCheckInterval_ThrowsAnException_WhenValueNonPositive(TimeSpan value)
        {
            var options = new EntityFrameworkJobStorageOptions();

            Assert.Throws<ArgumentOutOfRangeException>("value",
                () => options.JobExpirationCheckInterval = value);
        }

        [Fact]
        public void DefaultSchemaName_ThrowsAnException_WhenValueIsNull()
        {
            var options = new EntityFrameworkJobStorageOptions();

            Assert.Throws<ArgumentNullException>("value",
                () => options.DefaultSchemaName = null);
        }

        [Fact]
        public void DefaultSchemaName_SetChangesValue()
        {
            var options = new EntityFrameworkJobStorageOptions();
            var valueToSet = "TEST";

            options.DefaultSchemaName = valueToSet;

            Assert.Same(valueToSet, options.DefaultSchemaName);
        }

        public static IEnumerable<object[]> GetNonPositiveTimestamps()
        {
            yield return new object[] { TimeSpan.MinValue, };
            yield return new object[] { new TimeSpan(-1), };
            yield return new object[] { TimeSpan.Zero, };
        }
    }
}
