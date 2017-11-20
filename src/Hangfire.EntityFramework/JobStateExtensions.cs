// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.States;

namespace Hangfire.EntityFramework
{
    internal static class JobStateExtensions
    {
        private static IReadOnlyDictionary<JobState, string> StateNameMapping { get; } =
            new Dictionary<JobState, string>()
            {
                [JobState.Enqueued] = EnqueuedState.StateName,
                [JobState.Scheduled] = ScheduledState.StateName,
                [JobState.Processing] = ProcessingState.StateName,
                [JobState.Succeeded] = SucceededState.StateName,
                [JobState.Failed] = FailedState.StateName,
                [JobState.Deleted] = DeletedState.StateName,
                [JobState.Awaiting] = AwaitingState.StateName,
            };

        private static IReadOnlyDictionary<string, JobState> NameStateMapping { get; } =
            StateNameMapping.ToDictionary(x => x.Value, x => x.Key, StringComparer.Ordinal);

        public static string ToStateName(this JobState state)
        {
            if (!Enum.IsDefined(typeof(JobState), state))
                throw new ArgumentOutOfRangeException(nameof(state), state, string.Format(
                    ErrorStrings.Culture,
                    ErrorStrings.InvalidJobState,
                    state));

            string result;
            if (!StateNameMapping.TryGetValue(state, out result))
                throw new NotSupportedException(string.Format(
                    ErrorStrings.Culture,
                    ErrorStrings.JobStateNotSupported,
                    state));

            return result;
        }

        internal static JobState ToJobState(string stateName)
        {
            if (stateName == null)
                throw new ArgumentNullException(nameof(stateName));

            JobState result;
            if (!NameStateMapping.TryGetValue(stateName, out result))
                throw new NotSupportedException(string.Format(
                    ErrorStrings.Culture,
                    ErrorStrings.JobStateNotSupported,
                    stateName));

            return result;
        }
    }
}
