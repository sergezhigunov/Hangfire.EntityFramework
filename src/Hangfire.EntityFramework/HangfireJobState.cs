using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobState
    {
        [Key]
        public Guid StateId { get; set; }

        [ForeignKey(nameof(Job))]
        public Guid JobId { get; set; }

        [Required]
        [StringLength(20)]
        [Index("IX_HangfireJobState_Name", IsUnique = false)]
        public string Name { get; set; }

        [StringLength(100)]
        public string Reason { get; set; }

        public string Data { get; set; }

        [DateTimePrecision(7)]
        [Index("IX_HangfireJobState_CreatedAt", IsUnique = false)]
        public DateTime CreatedAt { get; set; }

        public virtual HangfireJob Job { get; set; }
    }
}
