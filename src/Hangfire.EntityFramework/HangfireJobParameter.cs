// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Hangfire.EntityFramework
{
    internal class HangfireJobParameter
    {
        [Key, Column(Order = 0)]
        [ForeignKey(nameof(Job))]
        public long JobId { get; set; }

        [Key, Column(Order = 1)]
        [Required]
        [StringLength(40)]
        public string Name { get; set; }

        public string Value { get; set; }

        public virtual HangfireJob Job { get; set; }
    }
}