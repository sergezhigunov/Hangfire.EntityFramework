using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobState
    {
        [Key, Column(Order = 0)]
        public Guid StateId { get; set; }

        public Guid JobId { get; set; }

        [Required]
        [StringLength(20)]
        public string Name { get; set; }

        [StringLength(100)]
        public string Reason { get; set; }

        public string Data { get; set; }

        [Index("IX_HangfireJobState_CreatedAt")]
        public DateTime CreatedAt { get; set; }

        [ForeignKey(nameof(JobId))]
        public virtual HangfireJob Job { get; set; }
    }
}
