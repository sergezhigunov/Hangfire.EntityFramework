﻿// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobQueueItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public long Id { get; set; }

        [ForeignKey(nameof(Job))]
        public long JobId { get; set; }

        [Required(AllowEmptyStrings = true)]
        [StringLength(50)]
        [Index(IsUnique = false)]
        public string Queue { get; set; }

        public virtual HangfireJob Job { get; set; }

        public virtual HangfireJobQueueItemLookup Lookup { get; set; }
    }
}
