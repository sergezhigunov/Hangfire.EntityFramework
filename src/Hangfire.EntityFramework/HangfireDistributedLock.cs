// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireDistributedLock
    {
        [Key]
        [StringLength(100)]
        public string Resource { get; set; }

        [DateTimePrecision(7)]
        [Index("IX_HangfireDistrLock_CreatedAt", IsUnique = false)]
        public DateTime CreatedAt { get; set; }
    }
}
