// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace Hangfire.EntityFramework
{
    internal class ScheduledStateData
    {
        public DateTime EnqueueAt { get; set; }

        public DateTime? ScheduledAt { get; set; }
    }
}
