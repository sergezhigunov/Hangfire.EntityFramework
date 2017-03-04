// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireServer
    {
        [Key]
        [MaxLength(100)]
        public string Id { get; set; }

        public string Data { get; set; }

        [Index("IX_HangfireServer_Heartbeat")]
        [DateTimePrecision(7)]
        public DateTime Heartbeat { get; set; }
    }
}