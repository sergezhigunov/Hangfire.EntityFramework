﻿// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    public class EntityFrameworkJobQueueMonitoringApiTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobQueueMonitoringApi(null));
        }

        [Fact, RollbackTransaction]
        public void GetQueues_ReturnsEmptyCollection_WhenQueuedItemsNotExisits()
        {
            var api = CreateMonitoringApi();

            var queues = api.GetQueues();

            Assert.Equal(0, queues.Count());
        }

        [Fact, RollbackTransaction]
        public void GetQueues_ReturnsAllGivenQueues_IfQueuesIsEmpty()
        {
            var date = DateTime.UtcNow;
            Guid[] jobIds = Enumerable.Repeat(0, 5).Select(x => Guid.NewGuid()).ToArray();
            var jobs = jobIds.Select(x => new HangfireJob
            {
                Id = x,
                CreatedAt = DateTime.UtcNow,
                InvocationData = string.Empty
            });
            var jobQueueItems = jobIds.Select(x => new HangfireJobQueueItem
            {
                Id = Guid.NewGuid(),
                CreatedAt = date += new TimeSpan(0, 0, 1),
                Queue = x.ToString(),
                JobId = x
            });
            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobQueues.AddRange(jobQueueItems);
            });

            var api = CreateMonitoringApi();

            var queues = api.GetQueues();

            Assert.Equal(5, queues.Count());
            Assert.All(queues, queue => Assert.True(jobIds.Contains(Guid.Parse(queue))));
        }

        [Fact, RollbackTransaction]
        public void GetEnqueuedJobIds_ReturnsEmptyCollection_IfQueueIsEmpty()
        {
            string queue = Guid.NewGuid().ToString();
            var api = CreateMonitoringApi();

            var result = api.GetEnqueuedJobIds(queue, 5, 15);

            Assert.Empty(result);
        }

        [Fact, RollbackTransaction]
        public void GetEnqueuedJobIds_ReturnsCorrectResult()
        {
            var date = DateTime.UtcNow;
            string queue = Guid.NewGuid().ToString();
            Guid[] jobIds = Enumerable.Repeat(0, 10).Select(x => Guid.NewGuid()).ToArray();
            var jobs = jobIds.Select(x => new HangfireJob
            {
                Id = x,
                CreatedAt = DateTime.UtcNow,
                InvocationData = string.Empty
            });
            var jobQueueItems = jobIds.Select(x => new HangfireJobQueueItem
            {
                Id = Guid.NewGuid(),
                CreatedAt = date += new TimeSpan(0, 0, 1),
                Queue = queue,
                JobId = x
            });

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobQueues.AddRange(jobQueueItems);
            });

            var api = CreateMonitoringApi();

            var result = api.GetEnqueuedJobIds(queue, 3, 2).ToArray();

            Assert.Equal(2, result.Length);
            Assert.Equal(jobIds[3], result[0]);
            Assert.Equal(jobIds[4], result[1]);
        }

        [Fact, RollbackTransaction]
        public void GetEnqueuedJobCount_ReturnsZeroes_WhenQueueNotExisits()
        {
            string queue = Guid.NewGuid().ToString();
            var api = CreateMonitoringApi();

            var result = api.GetEnqueuedJobCount(queue);

            Assert.Equal(0, result);
        }

        [Fact, RollbackTransaction]
        public void GetEnqueuedJobCount_ReturnsCorrectCounters()
        {
            var date = DateTime.UtcNow;
            string queue = Guid.NewGuid().ToString();
            Guid[] jobIds = Enumerable.Repeat(0, 3).Select(x => Guid.NewGuid()).ToArray();
            var jobs = jobIds.Select(x => new HangfireJob
            {
                Id = x,
                CreatedAt = DateTime.UtcNow,
                InvocationData = string.Empty
            });
            var jobQueueItems = jobIds.Select(x => new HangfireJobQueueItem
            {
                Id = Guid.NewGuid(),
                CreatedAt = date += new TimeSpan(0, 0, 1),
                Queue = queue,
                JobId = x
            }).ToArray();
            jobQueueItems.Last().Queue = Guid.NewGuid().ToString();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobQueues.AddRange(jobQueueItems);
            });

            var api = CreateMonitoringApi();

            var result = api.GetEnqueuedJobCount(queue);

            Assert.Equal(2, result);
        }

        private EntityFrameworkJobQueueMonitoringApi CreateMonitoringApi()
        {
            var storage = CreateStorage();
            return new EntityFrameworkJobQueueMonitoringApi(storage);
        }
    }
}
