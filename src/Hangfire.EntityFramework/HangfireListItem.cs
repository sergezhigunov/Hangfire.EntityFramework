using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireListItem
    {
        [Key, Column(Order = 0)]
        [Required]
        [StringLength(100)]
        public string Key { get; set; }

        [Key, Column(Order = 1)]
        public int Position { get; set; }

        public string Value { get; set; }

        [DateTimePrecision(7)]
        public DateTime? ExpireAt { get; set; }
    }
}
