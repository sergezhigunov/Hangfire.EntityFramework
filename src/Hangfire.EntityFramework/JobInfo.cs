// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

namespace Hangfire.EntityFramework
{
    internal class JobInfo
    {
        public long Id { get; set; }
        public string ClrType { get; set; }
        public string Method { get; set; }
        public string ArgumentTypes { get; set; }
        public string Arguments { get; set; }
        public string StateData { get; set; }
        public string StateReason { get; set; }
        public string State { get; set; }
    }
}