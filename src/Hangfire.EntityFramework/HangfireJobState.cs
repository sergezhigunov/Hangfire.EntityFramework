// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobState
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public long Id { get; set; }

        [ForeignKey(nameof(Job))]
        public long JobId { get; set; }

        [Required]
        [Index(IsUnique = false)]
        [StringLength(20)]
        public string Name { get; set; }

        [StringLength(100)]
        public string Reason { get; set; }

        public string Data { get; set; }

        [DateTimePrecision(7)]
        [Index(IsUnique = false)]
        public DateTime CreatedAt { get; set; }

        public virtual HangfireJob Job { get; set; }
    }
}
