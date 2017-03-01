using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobActualState
    {
        [Key, ForeignKey(nameof(Job))]
        public Guid JobId { get; set; }

        [ForeignKey(nameof(State))]
        public Guid StateId { get; set; }

        public virtual HangfireJob Job { get; set; }

        public virtual HangfireJobState State { get; set; }
    }
}
