// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireCounter
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public long Id { get; set; }

        [Required]
        [StringLength(100)]
        [Index(IsUnique = false)]
        public string Key { get; set; }

        public long Value { get; set; }

        [DateTimePrecision(7)]
        [Index(IsUnique = false)]
        public DateTime? ExpireAt { get; set; }
    }
}
