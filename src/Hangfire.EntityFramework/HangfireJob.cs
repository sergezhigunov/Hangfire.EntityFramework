// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Hangfire.EntityFramework
{
    internal class HangfireJob
    {
        [Key]
        public Guid Id { get; set; }

        [Required(AllowEmptyStrings = true)]
        public string InvocationData { get; set; }

        [DateTimePrecision(7)]
        [Index("IX_HangfireJob_CreatedAt", IsUnique = false)]
        public DateTime CreatedAt { get; set; }

        [DateTimePrecision(7)]
        [Index("IX_HangfireJob_ExpireAt", IsUnique = false)]
        public DateTime? ExpireAt { get; set; }

        public virtual HangfireJobActualState ActualState { get; set; }

        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<HangfireJobParameter> Parameters { get; set; } = new HashSet<HangfireJobParameter>();

        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<HangfireJobState> States { get; set; } = new HashSet<HangfireJobState>();
    }
}