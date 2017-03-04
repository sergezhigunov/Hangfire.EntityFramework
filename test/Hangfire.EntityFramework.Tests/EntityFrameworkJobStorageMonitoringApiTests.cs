// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Hangfire.Common;
using Hangfire.EntityFramework.Utils;
using Hangfire.States;
using Moq;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class EntityFrameworkJobStorageMonitoringApiTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobStorageMonitoringApi(null));
        }

        [Fact, CleanDatabase]
        public void Queues_ReturnsCorrectList()
        {
            Guid jobId = Guid.NewGuid();
            Guid stateId = Guid.NewGuid();

            var job = new HangfireJob
            {
                Id = jobId,
                CreatedAt = DateTime.UtcNow,
                InvocationData = string.Empty,
            };
            var jobState = new HangfireJobState
            {
                Id = stateId,
                JobId = jobId,
                CreatedAt = DateTime.UtcNow,
                Name = "State",
            };
            var jobQueueItem = new HangfireJobQueueItem
            {
                Id = Guid.NewGuid(),
                CreatedAt = DateTime.UtcNow,
                JobId = jobId,
                Queue = "default",
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobStates.Add(jobState);
                context.JobActualStates.Add(new HangfireJobActualState { JobId = jobId, StateId = stateId });
                context.JobQueues.Add(jobQueueItem);
            });

            var result = UseMonitoringApi(api => api.Queues());

            Assert.Equal(1, result.Count);
            Assert.Equal("default", result.First().Name);

        }

        [Fact, CleanDatabase]
        public void Servers_ReturnsCorrectList()
        {
            string serverId1 = "server1";
            string serverId2 = "server2";
            var workerCount = 4;
            var startedAt = new DateTime(2017, 1, 1, 11, 22, 33, DateTimeKind.Utc);
            var heartbeat = new DateTime(2017, 2, 2, 22, 33, 44, DateTimeKind.Utc);
            var queues = new[] { "critical", "default" };
            var data = new ServerData
            {
                WorkerCount = workerCount,
                Queues = queues,
                StartedAt = startedAt,
            };

            var servers = new[]
            {
                new HangfireServer { Id = serverId1, Heartbeat = heartbeat, Data = JobHelper.ToJson(data),  },
                new HangfireServer { Id = serverId2, Heartbeat = heartbeat,  },
            };

            UseContextWithSavingChanges(context => context.Servers.AddRange(servers));

            var result = UseMonitoringApi(api => api.Servers());

            Assert.Equal(2, servers.Length);
            var server1 = result.Single(x => x.Name == serverId1);
            var server2 = result.Single(x => x.Name == serverId2);
            Assert.Equal(heartbeat, server1.Heartbeat);
            Assert.Equal(workerCount, server1.WorkersCount);
            Assert.Equal(queues, server1.Queues);
            Assert.Equal(startedAt, server1.StartedAt);
            Assert.Equal(heartbeat, server2.Heartbeat);
            Assert.Equal(0, server2.WorkersCount);
            Assert.Null(server2.Queues);
            Assert.Equal(default(DateTime), server2.StartedAt);
        }

        [Fact, CleanDatabase]
        public void GetStatistics_ReturnsZeroes_WhenDatabaseClean()
        {
            var result = UseMonitoringApi(api => api.GetStatistics());

            Assert.NotNull(result);
            Assert.Equal(0, result.Deleted);
            Assert.Equal(0, result.Enqueued);
            Assert.Equal(0, result.Failed);
            Assert.Equal(0, result.Processing);
            Assert.Equal(0, result.Queues);
            Assert.Equal(0, result.Recurring);
            Assert.Equal(0, result.Scheduled);
            Assert.Equal(0, result.Servers);
            Assert.Equal(0, result.Succeeded);
        }

        [Theory, CleanDatabase]
        [InlineData(null)]
        [InlineData("1")]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        public void JobDetails_ReturnsNull_WhenJobNotExists(string jobId)
        {
            var result = UseMonitoringApi(api => api.JobDetails(jobId));

            Assert.Null(result);
        }

        [Fact, CleanDatabase]
        public void JobDetails_ReturnsCorrectResult()
        {
            var timestamp = DateTime.UtcNow;
            var createdAt = new DateTime(2012, 12, 12, 12,12,12, DateTimeKind.Unspecified);
            var stateData = new Dictionary<string, string> { { "Name", "Value" } };
            var state = new Mock<IState>();
            state.Setup(x => x.Name).Returns("State");
            state.Setup(x => x.Reason).Returns("Reason");
            state.Setup(x => x.SerializeData()).Returns(stateData);

            var jobParameters = new Dictionary<string, string> { ["Key1"] = "Value1", ["Key2"] = "Value2", };
            var jobId = UseConnection(connection =>
            {
                var id = connection.CreateExpiredJob(
                    Job.FromExpression(() => SampleMethod("argument")),
                    jobParameters,
                    createdAt,
                    TimeSpan.FromDays(1));

                using (var transaction = connection.CreateWriteTransaction())
                {
                    transaction.SetJobState(id, state.Object);
                    transaction.Commit();
                }

                return id;
            });

            var result = UseMonitoringApi(api => api.JobDetails(jobId));

            Assert.NotNull(result);
            Assert.Equal(createdAt, result.CreatedAt);
            Assert.Equal(createdAt.AddDays(1), result.ExpireAt);
            Assert.Equal(typeof(EntityFrameworkJobStorageMonitoringApiTests), result.Job.Type);
            Assert.Equal(nameof(SampleMethod), result.Job.Method.Name);
            Assert.Equal(new[] { "argument" }, result.Job.Args);
            Assert.NotNull(result.History);
            var historyItem = result.History.Single();
            Assert.Equal("State", historyItem.StateName);
            Assert.Equal("Reason", historyItem.Reason);
            Assert.True(historyItem.CreatedAt >= timestamp);
            Assert.Equal(stateData, historyItem.Data);
            Assert.NotNull(result.Properties);
            Assert.Equal(jobParameters, result.Properties);
        }

        private T UseMonitoringApi<T>(Func<EntityFrameworkJobStorageMonitoringApi, T> func)
        {
            string connectionString = ConnectionUtils.GetConnectionString();
            var storage = new EntityFrameworkJobStorage(connectionString);
            var monitoringApi = new EntityFrameworkJobStorageMonitoringApi(storage);
            return func(monitoringApi);
        }

        private void UseContextWithSavingChanges(Action<HangfireDbContext> action)
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            storage.UseHangfireDbContext(context =>
            {
                action(context);
                context.SaveChanges();
            });
        }

        private void UseConnection(Action<EntityFrameworkJobStorageConnection> action)
        {
            string connectionString = ConnectionUtils.GetConnectionString();
            var storage = new EntityFrameworkJobStorage(connectionString);

            using (var connection = new EntityFrameworkJobStorageConnection(storage))
                action(connection);
        }

        private T UseConnection<T>(Func<EntityFrameworkJobStorageConnection, T> func)
        {
            T result = default(T);
            UseConnection(connection => { result = func(connection); });
            return result;
        }

        [ExcludeFromCodeCoverage]
        public void SampleMethod(string value)
        { }
    }
}
