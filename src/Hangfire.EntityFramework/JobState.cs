// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

namespace Hangfire.EntityFramework
{
    internal enum JobState : byte
    {
        Created = 0,
        Enqueued,
        Scheduled,
        Processing,
        Succeeded,
        Failed,
        Deleted,
        Awaiting,
    }
}
