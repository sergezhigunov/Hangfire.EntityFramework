using System;
using System.ComponentModel.DataAnnotations;

namespace Hangfire.EntityFramework
{
    internal class HangfireDistributedLock
    {
        [Key]
        [StringLength(100)]
        public string Resource { get; set; }

        public DateTime CreatedAt { get; set; }
    }
}
