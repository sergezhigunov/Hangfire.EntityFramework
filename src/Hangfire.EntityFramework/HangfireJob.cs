using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;

namespace Hangfire.EntityFramework
{
    internal class HangfireJob
    {
        [Key]
        public Guid JobId { get; set; }

        [Required(AllowEmptyStrings = true)]
        public string InvocationData { get; set; }

        [DateTimePrecision(7)]
        public DateTime CreatedAt { get; set; }

        [DateTimePrecision(7)]
        public DateTime? ExpireAt { get; set; }

        public virtual HangfireJobActualState ActualState { get; set; }

        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<HangfireJobParameter> Parameters { get; set; } = new HashSet<HangfireJobParameter>();

        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<HangfireJobState> States { get; set; } = new HashSet<HangfireJobState>();
    }
}