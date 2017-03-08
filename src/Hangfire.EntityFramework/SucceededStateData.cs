// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace Hangfire.EntityFramework
{
    internal class SucceededStateData
    {
        public DateTime? SucceededAt { get; set; }

        public long PerformanceDuration { get; set; }

        public long Latency { get; set; }

        public object Result { get; set; }
    }
}
