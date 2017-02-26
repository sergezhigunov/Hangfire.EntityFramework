using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireSet
    {
        [Key, Column(Order = 0)]
        [Required]
        [StringLength(100)]
        public string Key { get; set; }

        [Key, Column(Order = 1)]
        [Required]
        [StringLength(100)]
        public string Value { get; set; }

        public double Score { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime? ExpireAt { get; set; }
    }
}
