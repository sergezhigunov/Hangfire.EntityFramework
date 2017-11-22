// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireSet
    {
        [Key, Column(Order = 0)]
        [Required]
        [StringLength(100)]
        public string Key { get; set; }

        [Key, Column(Order = 1)]
        [Required]
        [StringLength(100)]
        public string Value { get; set; }

        [Index(IsUnique = false)]
        public double Score { get; set; }

        [DateTimePrecision(7)]
        [Index(IsUnique = false)]
        public DateTime CreatedAt { get; set; }

        [DateTimePrecision(7)]
        [Index(IsUnique = false)]
        public DateTime? ExpireAt { get; set; }
    }
}
