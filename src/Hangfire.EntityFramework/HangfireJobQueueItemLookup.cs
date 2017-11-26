// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobQueueItemLookup
    {
        [Key]
        [ForeignKey(nameof(QueueItem))]
        public long QueueItemId { get; set; }

        [ForeignKey(nameof(ServerHost))]
        public Guid ServerHostId { get; set; }

        public virtual HangfireJobQueueItem QueueItem { get; set; }

        public virtual HangfireServerHost ServerHost { get; set; }
    }
}
