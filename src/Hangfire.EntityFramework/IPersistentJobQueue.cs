// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

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
