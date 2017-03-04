// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace Hangfire.EntityFramework
{
    internal interface IPersistentJobQueueMonitoringApi
    {
        string[] GetQueues();

        Guid[] GetEnqueuedJobIds(string queue, int from, int perPage);

        Guid[] GetFetchedJobIds(string queue, int from, int perPage);

        JobQueueCounters GetJobQueueCounters(string queue);
    }
}