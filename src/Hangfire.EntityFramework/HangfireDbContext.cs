// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity;
using System.Data.Entity.ModelConfiguration.Configuration;
using System.Data.Entity.ModelConfiguration.Conventions;

namespace Hangfire.EntityFramework
{
    internal class HangfireDbContext : DbContext
    {
        private string DefaultSchemaName { get; }

        public HangfireDbContext(string nameOrConnectionString, string defaultSchemaName)
            : base(nameOrConnectionString)
        {
            DefaultSchemaName = defaultSchemaName;
        }

        public DbSet<HangfireCounter> Counters { get; set; }

        public DbSet<HangfireDistributedLock> DistributedLocks { get; set; }

        public DbSet<HangfireHash> Hashes { get; set; }

        public DbSet<HangfireJob> Jobs { get; set; }

        public DbSet<HangfireJobActualState> JobActualStates { get; set; }

        public DbSet<HangfireJobState> JobStates { get; set; }

        public DbSet<HangfireJobParameter> JobParameters { get; set; }

        public DbSet<HangfireJobQueueItem> JobQueues { get; set; }

        public DbSet<HangfireListItem> Lists { get; set; }

        public DbSet<HangfireServer> Servers { get; set; }

        public DbSet<HangfireSet> Sets { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();
            modelBuilder.Conventions.Add<DateTimePrecisionConvention>();

            if (DefaultSchemaName != null)
                modelBuilder.HasDefaultSchema(DefaultSchemaName);
        }

        private class DateTimePrecisionConvention : PrimitivePropertyAttributeConfigurationConvention<DateTimePrecisionAttribute>
        {
            public override void Apply(ConventionPrimitivePropertyConfiguration configuration, DateTimePrecisionAttribute attribute)
            {
                if (attribute == null) throw new ArgumentNullException(nameof(attribute));
                if (configuration == null) throw new ArgumentNullException(nameof(configuration));

                var propertyType = configuration.ClrPropertyInfo.PropertyType;
                if (propertyType == typeof(DateTime) ||
                    propertyType == typeof(DateTime?) ||
                    propertyType == typeof(DateTimeOffset) ||
                    propertyType == typeof(DateTimeOffset))
                    configuration.HasPrecision(attribute.Value);
            }
        }
    }
}
