// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.EntityFramework
{
    internal class PersistentJobQueueProviderCollection : IEnumerable<IPersistentJobQueueProvider>
    {
        private Dictionary<string, IPersistentJobQueueProvider> ProvidersByQueue { get; }
            = new Dictionary<string, IPersistentJobQueueProvider>(StringComparer.OrdinalIgnoreCase);

        internal IPersistentJobQueueProvider DefaultProvider { get; }

        public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider defaultProvider)
        {
            if (defaultProvider == null)
                throw new ArgumentNullException(nameof(defaultProvider));

            DefaultProvider = defaultProvider;
        }

        public void Add(IPersistentJobQueueProvider provider, IEnumerable<string> queues)
        {
            if (provider == null)
                throw new ArgumentNullException(nameof(provider));

            if (queues == null)
                throw new ArgumentNullException(nameof(queues));

            foreach (var queue in queues)
                ProvidersByQueue.Add(queue, provider);
        }

        public virtual IPersistentJobQueueProvider GetProvider(string queue)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));

            return
                ProvidersByQueue.ContainsKey(queue) ?
                ProvidersByQueue[queue] :
                DefaultProvider;
        }

        public IEnumerator<IPersistentJobQueueProvider> GetEnumerator()
        {
            return Enumerable.Repeat(DefaultProvider, 1).
                Union(ProvidersByQueue.Values).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
