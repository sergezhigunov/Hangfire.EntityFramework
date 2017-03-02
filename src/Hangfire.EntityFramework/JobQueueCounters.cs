namespace Hangfire.EntityFramework
{
    internal class JobQueueCounters
    {
        public long EnqueuedCount { get; set; }
        public long FetchedCount { get; }
    }
}