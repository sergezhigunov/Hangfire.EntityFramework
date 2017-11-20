// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.EntityFramework.Utils;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    public class EntityFrameworkJobStorageConnectionTests
    {
        private Mock<IPersistentJobQueue> Queue { get; } = new Mock<IPersistentJobQueue>();
        private PersistentJobQueueProviderCollection Providers { get; }

        public EntityFrameworkJobStorageConnectionTests()
        {
            var provider = new Mock<IPersistentJobQueueProvider>();
            provider.Setup(x => x.GetJobQueue())
                .Returns(Queue.Object);

            Providers = new PersistentJobQueueProviderCollection(provider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobStorageConnection(null));
        }

        [Fact, RollbackTransaction]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("serverId",
                () => connection.AnnounceServer(null, new ServerContext())));
        }

        [Fact, RollbackTransaction]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("context",
                () => connection.AnnounceServer("server", null)));
        }

        [Fact, RollbackTransaction]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            string serverId = "server";
            var serverContext1 = new ServerContext
            {
                Queues = new[] { "CRITICAL", "DEFAULT" },
                WorkerCount = 4
            };
            var serverContext2 = new ServerContext
            {
                Queues = new[] { "DEFAULT" },
                WorkerCount = 1000
            };

            UseConnection(connection =>
            {
                DateTime timestampBeforeBegin = DateTime.UtcNow;
                connection.AnnounceServer(serverId, serverContext1);
                DateTime timestampAfterEnd = DateTime.UtcNow;

                CheckServer(serverId, serverContext1, timestampBeforeBegin, timestampAfterEnd);

                timestampBeforeBegin = DateTime.UtcNow;
                connection.AnnounceServer(serverId, serverContext2);
                timestampAfterEnd = DateTime.UtcNow;

                CheckServer(serverId, serverContext2, timestampBeforeBegin, timestampAfterEnd);
            });
        }

        private void CheckServer(string serverId, ServerContext actualContext, DateTime timestampBeforeBegin, DateTime timestampAfterEnd)
        {
            HangfireServer server = UseContext(context => context.Servers.Single(x => x.Id == serverId));
            var serverData = JobHelper.FromJson<ServerData>(server.Data);
            Assert.Equal(serverId, server.Id);
            Assert.Equal(actualContext.WorkerCount, serverData.WorkerCount);
            Assert.Equal(actualContext.Queues, serverData.Queues);
            Assert.True(timestampBeforeBegin <= serverData.StartedAt && serverData.StartedAt <= timestampAfterEnd);
        }

        [Fact, RollbackTransaction]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("serverId",
                () => connection.Heartbeat(null)));
        }

        [Fact, RollbackTransaction]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            string server1 = "server1";
            string server2 = "server2";

            DateTime datetime = new DateTime(2017, 1, 1, 11, 22, 33);

            var host = new HangfireServerHost { Id = EntityFrameworkJobStorage.ServerHostId };
            var servers = new[]
            {
                new HangfireServer { Id = server1, Heartbeat = datetime, ServerHost = host, },
                new HangfireServer { Id = server2, Heartbeat = datetime, ServerHost = host, },
            };

            UseContextWithSavingChanges(context =>
            {
                context.ServerHosts.Add(host);
                context.Servers.AddRange(servers);
            });

            UseConnection(connection => connection.Heartbeat("server1"));

            UseContext(context =>
            {
                Func<string, DateTime> getHeartbeatByServerId = serverId => (
                    from server in context.Servers
                    where server.Id == serverId
                    select server.Heartbeat).
                    Single();

                DateTime
                    actualHeartbeat1 = getHeartbeatByServerId(server1),
                    actualHeartbeat2 = getHeartbeatByServerId(server2);

                Assert.NotEqual(datetime, actualHeartbeat1);
                Assert.Equal(datetime, actualHeartbeat2);
            });
        }

        [Fact, RollbackTransaction]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("serverId",
                () => connection.RemoveServer(null)));
        }

        [Fact, RollbackTransaction]
        public void RemoveServer_RemovesAServerRecord()
        {
            var serverId = "Server1";

            var host = new HangfireServerHost { Id = EntityFrameworkJobStorage.ServerHostId,  };
            var server = new HangfireServer { Id = serverId, Heartbeat = DateTime.UtcNow, ServerHost = host, };

            UseContextWithSavingChanges(context =>
            {
                context.ServerHosts.Add(host);
                context.Servers.Add(server);
            });

            UseConnection(connection => connection.RemoveServer(serverId));

            UseContext(context =>
            {
                Assert.True(!context.ServerHosts.Any(x => x.Id == EntityFrameworkJobStorage.ServerHostId));
                Assert.True(!context.Servers.Any(x => x.Id == serverId));
            });
        }

        [Fact, RollbackTransaction]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            UseConnection(connection => Assert.Throws<ArgumentOutOfRangeException>("timeOut",
                () => connection.RemoveTimedOutServers(new TimeSpan(-1))));
        }

        [Fact, RollbackTransaction]
        public void RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            string server1 = "server1";
            string server2 = "server2";

            var host = new HangfireServerHost { Id = EntityFrameworkJobStorage.ServerHostId, };
            var servers = new[]
            {
                new HangfireServer { Id = server1, Heartbeat = DateTime.UtcNow.AddHours(-1), ServerHost = host, },
                new HangfireServer { Id = server2, Heartbeat = DateTime.UtcNow.AddHours(-3), ServerHost = host, },
            };
            UseContextWithSavingChanges(context =>
            {
                context.ServerHosts.Add(host);
                context.Servers.AddRange(servers);
            });

            UseConnection(connection => connection.RemoveTimedOutServers(TimeSpan.FromHours(2)));

            Assert.True(UseContext(context => context.Servers.Any(x => x.Id == server1)));
            Assert.False(UseContext(context => context.Servers.Any(x => x.Id == server2)));
        }

        [Fact, RollbackTransaction]
        public void CreateWriteTransaction_ReturnsEntityFrameworkJobStorageTransactionInstance()
        {
            var result = UseConnection(connection => connection.CreateWriteTransaction());

            Assert.NotNull(result);
            using (result)
                Assert.IsType<EntityFrameworkJobStorageTransaction>(result);
        }

        [Fact, RollbackTransaction]
        public void AcquireLock_ReturnsEntityFrameworkJobStorageDistributedLockInstance()
        {
            var result = UseConnection(connection => connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1)));

            Assert.NotNull(result);
            using (result)
                Assert.IsType<EntityFrameworkJobStorageDistributedLock>(result);
        }

        [Fact, RollbackTransaction]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("job",
                () => connection.CreateExpiredJob(
                    null,
                    new Dictionary<string, string>(),
                    DateTime.UtcNow,
                    TimeSpan.Zero)));
        }

        [Fact, RollbackTransaction]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("parameters",
                () => connection.CreateExpiredJob(
                    Job.FromExpression(() => SampleMethod("argument")),
                    null,
                    DateTime.UtcNow,
                    TimeSpan.Zero)));
        }

        [Fact, RollbackTransaction]
        public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            var createdAt = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc);

            var jobId = UseConnection(connection => connection.CreateExpiredJob(
                Job.FromExpression(() => SampleMethod("argument")),
                new Dictionary<string, string> { ["Key1"] = "Value1", ["Key2"] = "Value2", },
                createdAt,
                TimeSpan.FromDays(1)));

            Assert.NotNull(jobId);
            Assert.NotEmpty(jobId);

            HangfireJob hangfireJob = UseContext(context =>
                context.Jobs.
                Include(p => p.ActualState).
                Include(p => p.Parameters).
                Single());

            Assert.Equal(jobId, hangfireJob.Id.ToString());
            Assert.Equal(createdAt, hangfireJob.CreatedAt);
            Assert.Null(hangfireJob.ActualState);

            var invocationData = new InvocationData(
                hangfireJob.ClrType,
                hangfireJob.Method,
                hangfireJob.ParameterTypes,
                hangfireJob.Arguments);

            var job = invocationData.Deserialize();
            Assert.Equal(typeof(EntityFrameworkJobStorageConnectionTests), job.Type);
            Assert.Equal("SampleMethod", job.Method.Name);
            Assert.Equal("argument", job.Args[0]);
            Assert.True(createdAt.AddDays(1).AddMinutes(-1) < hangfireJob.ExpireAt);
            Assert.True(hangfireJob.ExpireAt < createdAt.AddDays(1).AddMinutes(1));
            var parameters = hangfireJob.Parameters.ToDictionary(x => x.Name, x => x.Value);
            Assert.Equal("Value1", parameters["Key1"]);
            Assert.Equal("Value2", parameters["Key2"]);
        }

        [Fact, RollbackTransaction]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("jobId",
                () => connection.GetJobData(null)));
        }

        [Theory, RollbackTransaction]
        [InlineData("1")]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob(string jobId)
        {
            var result = UseConnection(connection => connection.GetJobData(jobId));

            Assert.Null(result);
        }

        [Fact, RollbackTransaction]
        public void GetJobData_ReturnsResult_WhenJobExists()
        {
            var invocationData = JobUtils.CreateInvocationData(() => SampleMethod("Arguments"));
            var jobId = Guid.NewGuid();

            UseContextWithSavingChanges(context =>
            {
                var stateId = Guid.NewGuid();

                context.Jobs.Add(new HangfireJob
                {
                    Id = jobId,
                    ClrType = invocationData.Type,
                    Method = invocationData.Method,
                    ParameterTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                    CreatedAt = DateTime.UtcNow,
                });
                context.JobStates.Add(new HangfireJobState { Id = stateId, JobId = jobId, CreatedAt = DateTime.UtcNow, State = JobState.Succeeded, });
                context.JobActualStates.Add(new HangfireJobActualState { StateId = stateId, JobId = jobId, });
            });

            var result = UseConnection(connection => connection.GetJobData(jobId.ToString()));

            Assert.NotNull(result);
            Assert.NotNull(result.Job);
            Assert.Equal(SucceededState.StateName, result.State);
            Assert.Equal("Arguments", result.Job.Args[0]);
            Assert.Null(result.LoadException);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
            Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
        }

        [Fact, RollbackTransaction]
        public void GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
        {
            var invocationData = new InvocationData(null, null, null, string.Empty);
            var arguments = invocationData.Arguments;
            var jobId = Guid.NewGuid();

            UseContextWithSavingChanges(context =>
            {
                var stateId = Guid.NewGuid();

                context.Jobs.Add(new HangfireJob
                {
                    Id = jobId,
                    ClrType = invocationData.Type,
                    Method = invocationData.Type,
                    ParameterTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                    CreatedAt = DateTime.UtcNow,
                });
                context.JobStates.Add(new HangfireJobState { Id = stateId, JobId = jobId, CreatedAt = DateTime.UtcNow, State = JobState.Succeeded, });
                context.JobActualStates.Add(new HangfireJobActualState { StateId = stateId, JobId = jobId, });
            });

            var result = UseConnection(connection => connection.GetJobData(jobId.ToString()));

            Assert.NotNull(result.LoadException);
        }

        [Fact, RollbackTransaction]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("id",
                () => connection.SetJobParameter(null, "name", "value")));
        }

        [Fact, RollbackTransaction]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("name",
                () => connection.SetJobParameter("1", null, "value")));
        }

        [Fact, RollbackTransaction]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            var parameterName = Guid.NewGuid().ToString();
            var parameterValue = Guid.NewGuid().ToString();

            var jobId = Guid.NewGuid();
            var job = new HangfireJob { Id = jobId, CreatedAt = DateTime.UtcNow, };

            UseContextWithSavingChanges(context => context.Jobs.Add(job));

            UseConnection(connection => connection.SetJobParameter(jobId.ToString(), parameterName, parameterValue));

            var result = UseContext(context => (
                from parameter in context.JobParameters
                where parameter.JobId == jobId && parameter.Name == parameterName
                select parameter.Value).
                Single());

            Assert.Equal(parameterValue, result);
        }

        [Fact, RollbackTransaction]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            var parameterName = Guid.NewGuid().ToString();
            var parameterValue = Guid.NewGuid().ToString();
            var parameterAnotherValue = Guid.NewGuid().ToString();

            var jobId = Guid.NewGuid();
            var job = new HangfireJob { Id = jobId, CreatedAt = DateTime.UtcNow, };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobParameters.Add(new HangfireJobParameter { JobId = jobId, Name = parameterName, Value = parameterValue });
            });

            UseConnection(connection => connection.SetJobParameter(jobId.ToString(), parameterName, parameterAnotherValue));

            var result = UseContext(context => (
                from parameter in context.JobParameters
                where parameter.JobId == jobId && parameter.Name == parameterName
                select parameter.Value).
                Single());

            Assert.Equal(parameterAnotherValue, result);
        }

        [Fact, RollbackTransaction]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            var parameterName = Guid.NewGuid().ToString();

            var jobId = Guid.NewGuid();
            var job = new HangfireJob { Id = jobId, CreatedAt = DateTime.UtcNow, };

            UseContextWithSavingChanges(context => context.Jobs.Add(job));

            UseConnection(connection => connection.SetJobParameter(jobId.ToString(), parameterName, null));

            var result = UseContext(context => (
                from parameter in context.JobParameters
                where parameter.JobId == jobId && parameter.Name == parameterName
                select parameter.Value).
                Single());

            Assert.Null(result);
        }

        [Fact, RollbackTransaction]
        public void GetJobParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("id",
                () => connection.GetJobParameter(null, "name")));
        }

        [Fact, RollbackTransaction]
        public void GetJobParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("name",
                () => connection.GetJobParameter("1", null)));
        }

        [Theory, RollbackTransaction]
        [InlineData("1")]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        public void GetJobParameter_ReturnsNull_WhenParameterDoesNotExists(string jobId)
        {
            var value = UseConnection(connection => connection.GetJobParameter(jobId, "1"));

            Assert.Null(value);
        }

        [Fact, RollbackTransaction]
        public void GetJobParameter_ReturnsParameterValue_WhenJobExists()
        {
            var parameterName = Guid.NewGuid().ToString();
            var parameterValue = Guid.NewGuid().ToString();

            var jobId = Guid.NewGuid();
            var job = new HangfireJob { Id = jobId, CreatedAt = DateTime.UtcNow, };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobParameters.Add(new HangfireJobParameter { JobId = jobId, Name = parameterName, Value = parameterValue });
            });

            var value = UseConnection(connection => connection.GetJobParameter(jobId.ToString(), parameterName));

            Assert.Equal(parameterValue, value);
        }

        [Fact, RollbackTransaction]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("jobId",
               () => connection.GetStateData(null)));
        }

        [Theory, RollbackTransaction]
        [InlineData("1")]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState(string jobId)
        {
            var result = UseConnection(connection => connection.GetStateData(jobId));

            Assert.Null(result);
        }

        [Fact, RollbackTransaction]
        public void GetStateData_ReturnsCorrectData()
        {
            var invocationData = JobUtils.CreateInvocationData(() => SampleMethod("Arguments"));
            var jobId = Guid.NewGuid();
            var data = JobHelper.ToJson(new Dictionary<string, string> { ["Key"] = "Value", });

            UseContextWithSavingChanges(context =>
            {
                var stateId = Guid.NewGuid();

                context.Jobs.Add(new HangfireJob
                {
                    Id = jobId,
                    ClrType = invocationData.Type,
                    Method = invocationData.Type,
                    ParameterTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                    CreatedAt = DateTime.UtcNow,
                });
                context.JobStates.Add(new HangfireJobState
                {
                    Id = stateId,
                    JobId = jobId,
                    CreatedAt = DateTime.UtcNow,
                    State = JobState.Awaiting,
                    Reason = "Reason",
                    Data = data
                });
                context.JobActualStates.Add(new HangfireJobActualState { StateId = stateId, JobId = jobId, });
            });

            var result = UseConnection(connection => connection.GetStateData(jobId.ToString()));

            Assert.NotNull(result);

            Assert.Equal(AwaitingState.StateName, result.Name);
            Assert.Equal("Reason", result.Reason);
            Assert.Equal("Value", result.Data["Key"]);
        }

        [Fact, RollbackTransaction]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetAllItemsFromSet(null)));
        }

        [Fact, RollbackTransaction]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetAllItemsFromSet(key));

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact, RollbackTransaction]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            string setKey = Guid.NewGuid().ToString();

            var sets = new[]
            {
                new HangfireSet { Key = setKey, Value = "1", CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = setKey, Value = "2", CreatedAt = DateTime.UtcNow },
            };

            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            var result = UseConnection(connection => connection.GetAllItemsFromSet(setKey));

            Assert.Equal(sets.Length, result.Count);
            Assert.Contains("1", result);
            Assert.Contains("2", result);
        }

        [Fact, RollbackTransaction]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetFirstByLowestScoreFromSet(null, 0, 1)));
        }

        [Theory, RollbackTransaction]
        [InlineData(-1.0, 3.0)]
        [InlineData(3.0, -1.0)]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore(double fromScore, double toScore)
        {
            string set1 = Guid.NewGuid().ToString();
            string set2 = Guid.NewGuid().ToString();

            var sets = new[]
            {
                new HangfireSet { Key = set1, Value = "1.0", Score = 1.0,  CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set1, Value = "-1.0", Score = -1.0,  CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set1, Value = "-5.0", Score = -5.0,  CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set2, Value = "-2.0", Score = -2.0,  CreatedAt = DateTime.UtcNow },
            };

            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            var result = UseConnection(connection => connection.GetFirstByLowestScoreFromSet(set1, fromScore, toScore));

            Assert.Equal("-1.0", result);
        }

        [Fact, RollbackTransaction]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetSetCount(null));
            });
        }

        [Fact, RollbackTransaction]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetSetCount(key));

            Assert.Equal(0, result);
        }

        [Fact, RollbackTransaction]
        public void GetSetCount_ReturnsNumberOfElements_InASet()
        {
            string set1 = Guid.NewGuid().ToString();
            string set2 = Guid.NewGuid().ToString();

            var sets = new[]
            {
                new HangfireSet { Key = set1, Value = "1", CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set2, Value = "1", CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set1, Value = "2", CreatedAt = DateTime.UtcNow },
            };

            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            var result = UseConnection(connection => connection.GetSetCount(set1));

            Assert.Equal(2, result);
        }

        [Fact, RollbackTransaction]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetRangeFromSet(null, 0, 1)));
        }

        [Fact, RollbackTransaction]
        public void GetRangeFromSet_ReturnsPagedElements()
        {
            string set1 = Guid.NewGuid().ToString();
            string set2 = Guid.NewGuid().ToString();

            var sets = new[]
            {
                new HangfireSet { Key = set1, Value = "1", CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set1, Value = "2", CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set1, Value = "3", CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set1, Value = "4", CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set2, Value = "4", CreatedAt = DateTime.UtcNow },
                new HangfireSet { Key = set1, Value = "5", CreatedAt = DateTime.UtcNow },
            };

            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            var result = UseConnection(connection => connection.GetRangeFromSet(set1, 2, 3));

            Assert.Equal(new[] { "3", "4" }, result);
        }

        [Fact, RollbackTransaction]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetSetTtl(null)));
        }

        [Fact, RollbackTransaction]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetSetTtl(key));

            Assert.True(result < TimeSpan.Zero);
        }

        [Fact, RollbackTransaction]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            string set1 = Guid.NewGuid().ToString();
            string set2 = Guid.NewGuid().ToString();

            var sets = new[]
            {
                new HangfireSet { Key = set1, Value = "1", CreatedAt = DateTime.UtcNow, ExpireAt = DateTime.UtcNow.AddHours(1) },
                new HangfireSet { Key = set2, Value = "2", CreatedAt = DateTime.UtcNow },
            };

            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            var result = UseConnection(connection => connection.GetSetTtl(set1));

            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact, RollbackTransaction]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.SetRangeInHash(null, new Dictionary<string, string>())));
        }

        [Fact, RollbackTransaction]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("keyValuePairs",
                () => connection.SetRangeInHash("some-hash", null)));
        }

        [Fact, RollbackTransaction]
        public void SetRangeInHash_MergesAllRecords()
        {
            string key = Guid.NewGuid().ToString();

            UseConnection(connection => connection.SetRangeInHash(key, new Dictionary<string, string>
            {
                ["Key1"] = "Value1",
                ["Key2"] = "Value2",
            }));

            var result = UseContext(context => (
                from hash in context.Hashes
                where hash.Key == key
                select new { hash.Field, hash.Value }).
                ToDictionary(x => x.Field, x => x.Value));

            Assert.Equal("Value1", result["Key1"]);
            Assert.Equal("Value2", result["Key2"]);
        }

        [Fact, RollbackTransaction]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetAllEntriesFromHash(null)));
        }

        [Fact, RollbackTransaction]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetAllEntriesFromHash(key));

            Assert.Null(result);
        }

        [Fact, RollbackTransaction]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            string hash1 = Guid.NewGuid().ToString();
            string hash2 = Guid.NewGuid().ToString();

            var hangfireHashes = new[]
            {
                new HangfireHash { Key = hash1, Field = "Key1", Value = "Value1", },
                new HangfireHash { Key = hash1, Field = "Key2", Value = "Value2", },
                new HangfireHash { Key = hash2, Field = "Key3", Value = "Value3", },
            };

            UseContextWithSavingChanges(context => context.Hashes.AddRange(hangfireHashes));

            var result = UseConnection(connection => connection.GetAllEntriesFromHash(hash1));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.Equal("Value1", result["Key1"]);
            Assert.Equal("Value2", result["Key2"]);
        }

        [Fact, RollbackTransaction]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetHashCount(null)));
        }

        [Fact, RollbackTransaction]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetHashCount(key));

            Assert.Equal(0, result);
        }

        [Fact, RollbackTransaction]
        public void GetHashCount_ReturnsNumber_OfHashFields()
        {
            string hash1 = Guid.NewGuid().ToString();
            string hash2 = Guid.NewGuid().ToString();

            var hangfireHashes = new[]
            {
                new HangfireHash { Key = hash1, Field = "field-1", },
                new HangfireHash { Key = hash1, Field = "field-2", },
                new HangfireHash { Key = hash2, Field = "field-1", },
            };

            UseContextWithSavingChanges(context => context.Hashes.AddRange(hangfireHashes));

            var result = UseConnection(connection => connection.GetHashCount(hash1));

            Assert.Equal(2, result);
        }

        [Fact, RollbackTransaction]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetHashTtl(null)));
        }

        [Fact, RollbackTransaction]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetHashTtl(key));

            Assert.True(result < TimeSpan.Zero);
        }

        [Fact, RollbackTransaction]
        public void GetHashTtl_ReturnsExpirationTimeForHash()
        {
            string hash1 = Guid.NewGuid().ToString();
            string hash2 = Guid.NewGuid().ToString();

            var hangfireHashes = new[]
            {
                new HangfireHash { Key = hash1, Field = "field", ExpireAt = DateTime.UtcNow.AddHours(1) },
                new HangfireHash { Key = hash2, Field = "field", },
            };

            UseContextWithSavingChanges(context => context.Hashes.AddRange(hangfireHashes));

            var result = UseConnection(connection => connection.GetHashTtl(hash1));

            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact, RollbackTransaction]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetValueFromHash(null, "name")));

        }

        [Fact, RollbackTransaction]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("name",
                () => connection.GetValueFromHash("key", null)));
        }

        [Fact, RollbackTransaction]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetValueFromHash(key, "name"));

            Assert.Null(result);
        }

        [Fact, RollbackTransaction]
        public void GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            string hash1 = Guid.NewGuid().ToString();
            string hash2 = Guid.NewGuid().ToString();

            var hangfireHashes = new[]
            {
                new HangfireHash { Key = hash1, Field = "field-1", Value = "1", },
                new HangfireHash { Key = hash1, Field = "field-2", Value = "2", },
                new HangfireHash { Key = hash2, Field = "field-1", Value = "3", },
            };

            UseContextWithSavingChanges(context => context.Hashes.AddRange(hangfireHashes));

            var result = UseConnection(connection => connection.GetValueFromHash(hash1, "field-1"));

            Assert.Equal("1", result);
        }

        [Fact, RollbackTransaction]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetCounter(null)));
        }

        [Fact, RollbackTransaction]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetCounter(key));

            Assert.Equal(0, result);
        }

        [Fact, RollbackTransaction]
        public void GetCounter_ReturnsSumOfValues_InCounterTable()
        {
            string key1 = Guid.NewGuid().ToString();
            string key2 = Guid.NewGuid().ToString();

            var counters = new[]
            {
                new HangfireCounter { Id = Guid.NewGuid(), Key = key1, Value = 1, },
                new HangfireCounter { Id = Guid.NewGuid(), Key = key2, Value = 1, },
                new HangfireCounter { Id = Guid.NewGuid(), Key = key1, Value = 1, },
            };

            UseContextWithSavingChanges(context => context.Counters.AddRange(counters));

            var result = UseConnection(connection => connection.GetCounter(key1));

            Assert.Equal(2, result);
        }

        [Fact, RollbackTransaction]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetListCount(null)));
        }

        [Fact, RollbackTransaction]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetListCount(key));

            Assert.Equal(0, result);
        }

        [Fact, RollbackTransaction]
        public void GetListCount_ReturnsTheNumberOfListElements()
        {
            string list1 = Guid.NewGuid().ToString();
            string list2 = Guid.NewGuid().ToString();

            var lists = new[]
            {
                new HangfireListItem { Key = list1, Position = 0, },
                new HangfireListItem { Key = list1, Position = 1, },
                new HangfireListItem { Key = list2, Position = 0, },
            };

            UseContextWithSavingChanges(context => context.Lists.AddRange(lists));

            var result = UseConnection(connection => connection.GetListCount(list1));

            Assert.Equal(2, result);
        }

        [Fact, RollbackTransaction]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.GetListTtl(null)));
        }

        [Fact, RollbackTransaction]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetListTtl(key));

            Assert.True(result < TimeSpan.Zero);
        }

        [Fact, RollbackTransaction]
        public void GetListTtl_ReturnsExpirationTimeForList()
        {
            string list1 = Guid.NewGuid().ToString();
            string list2 = Guid.NewGuid().ToString();

            var lists = new[]
            {
                new HangfireListItem { Key = list1, Position = 0, ExpireAt = DateTime.UtcNow.AddHours(1) },
                new HangfireListItem { Key = list2, Position = 0, },
            };

            UseContextWithSavingChanges(context => context.Lists.AddRange(lists));

            var result = UseConnection(connection => connection.GetListTtl(list1));

            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact, RollbackTransaction]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetRangeFromList(null, 0, 1)));
        }

        [Fact, RollbackTransaction]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();

            var result = UseConnection(connection => connection.GetRangeFromList(key, 0, 1));

            Assert.Empty(result);
        }

        [Fact, RollbackTransaction]
        public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            string list1 = Guid.NewGuid().ToString();
            string list2 = Guid.NewGuid().ToString();

            var lists = new[]
            {
                new HangfireListItem { Key = list1, Position = 0, Value = "1", },
                new HangfireListItem { Key = list2, Position = 0, Value = "2", },
                new HangfireListItem { Key = list1, Position = 1, Value = "3", },
                new HangfireListItem { Key = list1, Position = 2, Value = "4", },
                new HangfireListItem { Key = list1, Position = 3, Value = "5", },
            };

            UseContextWithSavingChanges(context => context.Lists.AddRange(lists));

            var result = UseConnection(connection => connection.GetRangeFromList(list1, 1, 2));

            Assert.Equal(new[] { "4", "3" }, result);
        }

        [Fact, RollbackTransaction]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("key",
                () => connection.GetAllItemsFromList(null)));
        }

        [Fact, RollbackTransaction]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            string key = Guid.NewGuid().ToString();
            var result = UseConnection(connection => connection.GetAllItemsFromList(key));

            Assert.Empty(result);
        }

        [Fact, RollbackTransaction]
        public void GetAllItemsFromList_ReturnsAllItems_FromAGivenList()
        {
            string list1 = Guid.NewGuid().ToString();
            string list2 = Guid.NewGuid().ToString();

            var lists = new[]
            {
                new HangfireListItem { Key = list1, Position = 0, Value = "1", },
                new HangfireListItem { Key = list2, Position = 0, Value = "2", },
                new HangfireListItem { Key = list1, Position = 1, Value = "3", },
            };

            UseContextWithSavingChanges(context => context.Lists.AddRange(lists));

            var result = UseConnection(connection => connection.GetAllItemsFromList(list1));

            Assert.Equal(new[] { "3", "1" }, result);
        }

        [Fact]
        public void FetchNextJob_Throws_IfQueuesIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>("queues",
                () => connection.FetchNextJob(null, new CancellationToken())));
        }

        [Fact]
        public void FetchNextJob_Throws_IfQueuesIsEmpty()
        {
            UseConnection(connection => Assert.Throws<ArgumentException>("queues",
                () => connection.FetchNextJob(new string[0], new CancellationToken())));
        }

        [Fact, RollbackTransaction]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            UseConnection(connection =>
            {
                var token = new CancellationToken();
                var queues = new[] { "DEFAULT" };

                connection.FetchNextJob(queues, token);

                Queue.Verify(x => x.Dequeue(queues, token));
            });
        }

        [Fact, RollbackTransaction]
        public void FetchNextJob_Throws_IfMultipleProvidersResolved()
        {
            UseConnection(connection =>
            {
                var token = new CancellationToken();
                var anotherProvider = new Mock<IPersistentJobQueueProvider>();
                Providers.Add(anotherProvider.Object, new[] { "CRITICAL" });

                Assert.Throws<InvalidOperationException>(
                    () => connection.FetchNextJob(new[] { "CRITICAL", "DEFAULT" }, token));
            });
        }

        private void UseConnection(Action<EntityFrameworkJobStorageConnection> action)
        {
            string connectionString = GetConnectionString();
            var storage = new Mock<EntityFrameworkJobStorage>(connectionString);
            storage.Setup(x => x.QueueProviders).Returns(Providers);

            using (var connection = new EntityFrameworkJobStorageConnection(storage.Object))
                action(connection);
        }

        private T UseConnection<T>(Func<EntityFrameworkJobStorageConnection, T> func)
        {
            T result = default(T);
            UseConnection(connection => { result = func(connection); });
            return result;
        }

        [ExcludeFromCodeCoverage]
        [SuppressMessage("Usage", "xUnit1013")]
        public void SampleMethod(string value)
        {
            Debug.WriteLine("SampleMethod executed. value = '{0}'", new[] { value });
        }
    }
}
