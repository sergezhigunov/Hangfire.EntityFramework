using System;
using System.ComponentModel.DataAnnotations;

namespace Hangfire.EntityFramework
{
    internal class HangfireDistributedLock
    {
        [Key]
        [StringLength(100)]
        public string Resource { get; set; }

        [DateTimePrecision(7)]
        public DateTime CreatedAt { get; set; }
    }
}
