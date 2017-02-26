using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobParameter
    {
        [Key, Column(Order = 0)]
        public Guid JobId { get; set; }

        [Key, Column(Order = 1)]
        [Required]
        [StringLength(40)]
        public string Name { get; set; }

        public string Value { get; set; }

        [ForeignKey(nameof(JobId))]
        public virtual HangfireJob Job { get; set; }
    }
}