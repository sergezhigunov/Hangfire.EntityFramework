using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireServer
    {
        [Key]
        [MaxLength(100)]
        public string ServerId { get; set; }

        public string Data { get; set; }

        [Index("IX_HangfireServer_Heartbeat")]
        public DateTime Heartbeat { get; set; }
    }
}