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