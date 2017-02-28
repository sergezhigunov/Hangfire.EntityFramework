using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Hangfire.EntityFramework
{
    internal class PersistentJobQueueProviderCollection : Collection<IPersistentJobQueueProvider>
    {
        private IPersistentJobQueueProvider DefaultProvider { get; }
        private readonly Dictionary<string, IPersistentJobQueueProvider> ProvidersByQueue
            = new Dictionary<string, IPersistentJobQueueProvider>(StringComparer.OrdinalIgnoreCase);


        public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider defaultProvider)
        {
            if (defaultProvider == null) throw new ArgumentNullException(nameof(defaultProvider));

            DefaultProvider = defaultProvider;
            Add(DefaultProvider);
        }

        public void Add(IPersistentJobQueueProvider provider, IEnumerable<string> queues)
        {
            if (provider == null) throw new ArgumentNullException(nameof(provider));
            if (queues == null) throw new ArgumentNullException(nameof(queues));

            Add(provider);

            foreach (var queue in queues)
                ProvidersByQueue.Add(queue, provider);
        }

        public IPersistentJobQueueProvider GetProvider(string queue)
        {
            return ProvidersByQueue.ContainsKey(queue)
                ? ProvidersByQueue[queue]
                : DefaultProvider;
        }
    }
}
