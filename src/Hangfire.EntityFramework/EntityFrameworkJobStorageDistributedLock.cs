using System;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobStorageDistributedLock : IDisposable
    {
        private EntityFrameworkJobStorage Storage { get; }
        private string Resource { get; }
        private TimeSpan Timeout { get; }

        public EntityFrameworkJobStorageDistributedLock(EntityFrameworkJobStorage storage, string resource, TimeSpan timeout)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            if (timeout < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout), timeout, null);

            Resource = resource;
            Timeout = timeout;
            Storage = storage;
        }

        public void Dispose()
        { }
    }
}