// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;

namespace Hangfire.EntityFramework
{
    internal class HangfireServerHost
    {
        [Key]
        public Guid Id { get; set; }

        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<HangfireServer> Servers { get; set; } = new HashSet<HangfireServer>();

        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<HangfireJobQueue> QueueItems { get; set; } = new HashSet<HangfireJobQueue>();
    }
}
