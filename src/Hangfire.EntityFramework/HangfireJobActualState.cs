// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobActualState
    {
        [Key, ForeignKey(nameof(Job))]
        public long JobId { get; set; }

        [ForeignKey(nameof(State))]
        public long StateId { get; set; }

        public virtual HangfireJob Job { get; set; }

        public virtual HangfireJobState State { get; set; }
    }
}
