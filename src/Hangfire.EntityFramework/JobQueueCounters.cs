namespace Hangfire.EntityFramework
{
    internal class JobQueueCounters
    {
        public string Queue { get; internal set; }
        public long EnqueuedCount { get; set; }
        public long FetchedCount { get; set; }
    }
}