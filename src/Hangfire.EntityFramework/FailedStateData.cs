// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace Hangfire.EntityFramework
{
    internal class FailedStateData
    {
        public DateTime? FailedAt { get; set; }

        public string ExceptionType { get; set; }

        public string ExceptionMessage { get; set; }

        public string ExceptionDetails { get; set; }
    }
}
