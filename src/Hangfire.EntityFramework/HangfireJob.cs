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
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public long Id { get; set; }

        [Index(IsUnique = false)]
        public JobState ActualState { get; set; }

        [StringLength(512)]
        public string ClrType { get; set; }

        [StringLength(512)]
        public string Method { get; set; }

        public string ArgumentTypes { get; set; }

        public string Arguments { get; set; }

        [DateTimePrecision(7)]
        [Index(IsUnique = false)]
        public DateTime CreatedAt { get; set; }

        [DateTimePrecision(7)]
        [Index(IsUnique = false)]
        public DateTime? ExpireAt { get; set; }

        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<HangfireJobParameter> Parameters { get; set; } = new HashSet<HangfireJobParameter>();

        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<HangfireJobState> States { get; set; } = new HashSet<HangfireJobState>();
    }
}