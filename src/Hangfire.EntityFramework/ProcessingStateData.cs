﻿// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace Hangfire.EntityFramework
{
    internal class ProcessingStateData
    {
        public DateTime? StartedAt { get; set; }

        public string ServerId { get; set; }

        public string ServerName { get; set; }

        public string WorkerId { get; set; }
    }
}
