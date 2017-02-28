using System;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobQueueProvider : IPersistentJobQueueProvider
    {
        private IPersistentJobQueue JobQueue { get; }

        private IPersistentJobQueueMonitoringApi MonitoringApi { get; }

        public EntityFrameworkJobQueueProvider(EntityFrameworkJobStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            JobQueue = new EntityFrameworkJobQueue(storage);
            MonitoringApi = new EntityFrameworkJobQueueMonitoringApi(storage);
        }

        public IPersistentJobQueue GetJobQueue() => JobQueue;

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi() => MonitoringApi;
    }
}
