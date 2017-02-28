using System.Threading;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal interface IPersistentJobQueue
    {
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);

        void Enqueue(string queue, string jobId);
    }
}
