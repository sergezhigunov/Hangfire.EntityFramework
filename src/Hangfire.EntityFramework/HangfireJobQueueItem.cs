﻿// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobQueueItem
    {
        [Key]
        public Guid Id { get; set; }

        [ForeignKey(nameof(Job))]
        public Guid JobId { get; set; }

        [Required(AllowEmptyStrings = true)]
        [StringLength(50)]
        [Index(IsUnique = false)]
        public string Queue { get; set; }

        [DateTimePrecision(7)]
        [Index(IsUnique = false)]
        public DateTime CreatedAt { get; set; }

        public virtual HangfireJob Job { get; set; }

        public virtual HangfireJobQueueItemLookup Lookup { get; set; }
    }
}
