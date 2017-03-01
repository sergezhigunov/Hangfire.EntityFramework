using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireCounter
    {
        [Key]
        public Guid Id { get; set; }

        [Required]
        [StringLength(100)]
        [Index("IX_HangfireCounter_Key", IsUnique = false)]
        public string Key { get; set; }

        public long Value { get; set; }

        [DateTimePrecision(7)]
        [Index("IX_HangfireCounter_ExpireAt", IsUnique = false)]
        public DateTime? ExpireAt { get; set; }
    }
}
