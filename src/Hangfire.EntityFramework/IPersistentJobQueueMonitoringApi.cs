// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.


namespace Hangfire.EntityFramework
{
    internal interface IPersistentJobQueueMonitoringApi
    {
        string[] GetQueues();

        long[] GetEnqueuedJobIds(string queue, int from, int perPage);

        long GetEnqueuedJobCount(string queue);
    }
}