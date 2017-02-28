using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobQueueItem
    {
        [Key]
        public Guid Id { get; set; }

        [ForeignKey(nameof(Job))]
        public Guid JobId { get; set; }

        [Required]
        [StringLength(50)]
        [Index("IX_HangfireJobQueueItem_Queue", IsUnique = false)]
        public string Queue { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime? FetchedAt { get; set; }

        public virtual HangfireJob Job { get; set; }
    }
}
