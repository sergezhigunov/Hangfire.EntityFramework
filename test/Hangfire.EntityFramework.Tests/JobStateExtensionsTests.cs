// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Hangfire.States;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class JobStateExtensionsTests
    {
        [Fact]
        public void ToStateName_Throws_WhenStateIsOutOfRange()
        {
            var state = (JobState)int.MaxValue;

            var exception = Assert.Throws<ArgumentOutOfRangeException>("state",
                () => state.ToStateName());

            Assert.Equal(state, exception.ActualValue);
        }

        [Theory]
        [MemberData(nameof(GetStateNameMapping))]
        public void ToStateName_ReturnsCorrectResult(Enum stateValue, string stateName)
        {
            var state = (JobState)stateValue;

            var actualStateName = JobStateExtensions.ToStateName(state);

            Assert.Equal(stateName, actualStateName);
        }
        [Fact]
        public void ToJobState_Throws_WhenStateIsNull()
        {
            Assert.Throws<ArgumentNullException>("stateName",
                () => JobStateExtensions.ToJobState(null));
        }

        [Fact]
        public void ToJobState_Throws_WhenStateIsUnknown()
        {
            string stateName = "a not existing state name";

            Assert.Throws<NotSupportedException>(
                () => JobStateExtensions.ToJobState(stateName));
        }

        [Theory]
        [MemberData(nameof(GetStateNameMapping))]
        public void ToJobState_ReturnsCorrectResult(Enum stateValue, string stateName)
        {
            var state = (JobState)stateValue;

            var actualState = JobStateExtensions.ToJobState(stateName);

            Assert.Equal(state, actualState);
        }

        public static IEnumerable<object[]> GetStateNameMapping()
        {
            yield return new object[] { JobState.Enqueued, EnqueuedState.StateName };
            yield return new object[] { JobState.Scheduled, ScheduledState.StateName };
            yield return new object[] { JobState.Processing, ProcessingState.StateName };
            yield return new object[] { JobState.Succeeded, SucceededState.StateName };
            yield return new object[] { JobState.Failed, FailedState.StateName };
            yield return new object[] { JobState.Deleted, DeletedState.StateName };
            yield return new object[] { JobState.Awaiting, AwaitingState.StateName };
        }
    }
}
