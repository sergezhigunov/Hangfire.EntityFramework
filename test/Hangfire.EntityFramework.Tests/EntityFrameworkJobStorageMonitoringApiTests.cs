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
    using static ConnectionUtils;

    public class EntityFrameworkJobStorageMonitoringApiTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobStorageMonitoringApi(null));
        }

        [Fact, CleanDatabase]
        public void Queues_ReturnsEmptyList_WhenNoQueuesExists()
        {
            var result = UseMonitoringApi(api => api.Queues());

            Assert.NotNull(result);
            Assert.Equal(0, result.Count);
        }

        [Fact, CleanDatabase]
        public void Queues_ReturnsCorrectList()
        {
            Guid stateId = Guid.NewGuid();

            var job = new HangfireJob
            {
                Id = Guid.NewGuid(),
                CreatedAt = DateTime.UtcNow,
                InvocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test")),
            };
            var state = new HangfireJobState
            {
                Id = Guid.NewGuid(),
                Job = job,
                CreatedAt = DateTime.UtcNow,
                Name = "State",
            };
            var jobQueueItem = new HangfireJobQueueItem
            {
                Id = Guid.NewGuid(),
                CreatedAt = DateTime.UtcNow,
                Job = job,
                Queue = "default",
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobStates.Add(state);
                context.JobActualStates.Add(new HangfireJobActualState { Job = job, State = state });
                context.JobQueues.Add(jobQueueItem);
            });

            var result = UseMonitoringApi(api => api.Queues());

            Assert.NotNull(result);
            Assert.Equal(1, result.Count);
            var firstItem = result.First();
            Assert.Equal("default", firstItem.Name);
            Assert.Equal(null, firstItem.Fetched);
            Assert.Equal(1, firstItem.Length);
            Assert.Equal(1, firstItem.FirstJobs.Count());
            var firstJobKeyValuePair = firstItem.FirstJobs.First();
            Assert.Equal(job.Id.ToString(), firstJobKeyValuePair.Key);
            var firstJob = firstJobKeyValuePair.Value;
            Assert.Equal(null, firstJob.EnqueuedAt);
            Assert.Equal(true, firstJob.InEnqueuedState);
            Assert.Equal("State", firstJob.State);
            Assert.Equal(job.InvocationData, JobUtils.CreateInvocationData(firstJob.Job));
        }

        [Fact, CleanDatabase]
        public void Servers_ReturnsEmptyList_WhenNoServersExists()
        {
            var result = UseMonitoringApi(api => api.Servers());

            Assert.NotNull(result);
            Assert.Equal(0, result.Count);
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
            var data = JobHelper.ToJson(new ServerData
            {
                WorkerCount = workerCount,
                Queues = queues,
                StartedAt = startedAt,
            });
            var host = new HangfireServerHost { Id = EntityFrameworkJobStorage.ServerHostId, };
            var servers = new[]
            {
                new HangfireServer { Id = serverId1, Heartbeat = heartbeat, Data = data, ServerHost = host,  },
                new HangfireServer { Id = serverId2, Heartbeat = heartbeat, ServerHost = host,  },
            };

            UseContextWithSavingChanges(context =>
            {
                context.ServerHosts.Add(host);
                context.Servers.AddRange(servers);
            });

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

        [Fact, CleanDatabase]
        public void GetStatistics_ReturnsCorrectCounts()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 1; i++)
                    AddJobWithStateToContext(context, EnqueuedState.StateName);
                for (int i = 0; i < 2; i++)
                    AddJobWithStateToContext(context, FailedState.StateName);
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, ProcessingState.StateName);
                for (int i = 0; i < 4; i++)
                    AddJobWithStateToContext(context, ScheduledState.StateName);
                context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = "stats:deleted", Value = 5 });
                context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = "stats:succeeded", Value = 6 });
                for (int i = 0; i < 7; i++)
                    context.Sets.Add(new HangfireSet { Key = "recurring-jobs", Value = Guid.NewGuid().ToString(), CreatedAt = DateTime.UtcNow, });
                var host = context.ServerHosts.Add(new HangfireServerHost { Id = EntityFrameworkJobStorage.ServerHostId, });
                for (int i = 0; i < 8; i++)
                    context.Servers.Add(new HangfireServer { Id = Guid.NewGuid().ToString(), Heartbeat = DateTime.UtcNow, ServerHostId = host.Id });
                for (int i = 0; i < 9; i++)
                    AddJobWithQueueItemToContext(context, ScheduledState.StateName, Guid.NewGuid().ToString());
            });
            var result = UseMonitoringApi(api => api.GetStatistics());

            Assert.NotNull(result);
            Assert.Equal(5, result.Deleted);
            Assert.Equal(1, result.Enqueued);
            Assert.Equal(2, result.Failed);
            Assert.Equal(3, result.Processing);
            Assert.Equal(9, result.Queues);
            Assert.Equal(7, result.Recurring);
            Assert.Equal(4, result.Scheduled);
            Assert.Equal(8, result.Servers);
            Assert.Equal(6, result.Succeeded);
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
            var createdAt = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Unspecified);
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

        [Fact, CleanDatabase]
        public void SucceededByDatesCount_ReturnsCorrectResult()
        {
            var today = DateTime.UtcNow.Date;
            var counts = Enumerable.Range(0, 7);
            var dictionaryDates = counts.ToDictionary(x => today.AddDays(-x));
            UseContextWithSavingChanges(context =>
            {
                foreach (var item in dictionaryDates)
                    if (item.Value != 0)
                        context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = $"stats:succeeded:{item.Key:yyyy-MM-dd}", Value = item.Value });
            });

            var result = UseMonitoringApi(api => api.SucceededByDatesCount());

            Assert.NotNull(result);
            Assert.Equal(7, result.Count);
            Assert.All(result, item => Assert.Equal(dictionaryDates[item.Key], item.Value));
        }

        [Fact, CleanDatabase]
        public void FailedByDatesCount_ReturnsCorrectResult()
        {
            var today = DateTime.UtcNow.Date;
            var counts = Enumerable.Range(0, 7);
            var dictionaryDates = counts.ToDictionary(x => today.AddDays(-x));
            UseContextWithSavingChanges(context =>
            {
                foreach (var item in dictionaryDates)
                    if (item.Value != 0)
                        context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = $"stats:failed:{item.Key:yyyy-MM-dd}", Value = item.Value });
            });

            var result = UseMonitoringApi(api => api.FailedByDatesCount());

            Assert.NotNull(result);
            Assert.Equal(7, result.Count);
            Assert.All(result, item => Assert.Equal(dictionaryDates[item.Key], item.Value));
        }

        [Fact, CleanDatabase]
        public void HourlySucceededJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;
            var counts = Enumerable.Range(0, 24);
            var dictionaryDates = counts.ToDictionary(x =>
            {
                var hour = now.AddHours(-x);
                return new DateTime(hour.Year, hour.Month, hour.Day, hour.Hour, 0, 0, DateTimeKind.Utc);
            });
            UseContextWithSavingChanges(context =>
            {
                foreach (var item in dictionaryDates)
                    if (item.Value != 0)
                        context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = $"stats:succeeded:{item.Key:yyyy-MM-dd-HH}", Value = item.Value });
            });

            var result = UseMonitoringApi(api => api.HourlySucceededJobs());

            Assert.NotNull(result);
            Assert.Equal(24, result.Count);
            Assert.All(result, item => Assert.Equal(dictionaryDates[item.Key], item.Value));
        }

        [Fact, CleanDatabase]
        public void HourlyFailedJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;
            var counts = Enumerable.Range(0, 24);
            var dictionaryDates = counts.ToDictionary(x =>
            {
                var hour = now.AddHours(-x);
                return new DateTime(hour.Year, hour.Month, hour.Day, hour.Hour, 0, 0, DateTimeKind.Utc);
            });
            UseContextWithSavingChanges(context =>
            {
                foreach (var item in dictionaryDates)
                    if (item.Value != 0)
                        context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = $"stats:failed:{item.Key:yyyy-MM-dd-HH}", Value = item.Value });
            });

            var result = UseMonitoringApi(api => api.HourlyFailedJobs());

            Assert.NotNull(result);
            Assert.Equal(24, result.Count);
            Assert.All(result, item => Assert.Equal(dictionaryDates[item.Key], item.Value));
        }

        [Fact, CleanDatabase]
        public void ScheduledCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, ScheduledState.StateName);
            });

            var result = UseMonitoringApi(api => api.ScheduledCount());

            Assert.Equal(3, result);
        }

        [Fact, CleanDatabase]
        public void FailedCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, FailedState.StateName);
            });

            var result = UseMonitoringApi(api => api.FailedCount());

            Assert.Equal(3, result);
        }

        [Fact, CleanDatabase]
        public void ProcessingCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, ProcessingState.StateName);
            });

            var result = UseMonitoringApi(api => api.ProcessingCount());

            Assert.Equal(3, result);
        }

        [Fact, CleanDatabase]
        public void SucceededListCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, SucceededState.StateName);
            });

            var result = UseMonitoringApi(api => api.SucceededListCount());

            Assert.Equal(3, result);
        }

        [Fact, CleanDatabase]
        public void DeletedListCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, DeletedState.StateName);
            });

            var result = UseMonitoringApi(api => api.DeletedListCount());

            Assert.Equal(3, result);
        }

        [Fact, CleanDatabase]
        public void EnqueuedCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithQueueItemToContext(context, EnqueuedState.StateName, "queue");
            });

            var result = UseMonitoringApi(api => api.EnqueuedCount("queue"));

            Assert.Equal(3, result);
        }

        [Fact, CleanDatabase]
        public void FetchedCount_ReturnsZero()
        {
            var result = UseMonitoringApi(api => api.FetchedCount("queue"));

            Assert.Equal(0, result);
        }

        [Fact, CleanDatabase]
        public void SucceededJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                { "SucceededAt",  JobHelper.SerializeDateTime(now) },
                { "PerformanceDuration", "123" },
                { "Latency", "456" },
                { "Result", "789" },
            };

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    InvocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test")),
                }).
                ToArray();
            var states = jobs.Select(x => new HangfireJobState
            {
                Id = Guid.NewGuid(),
                Job = x,
                CreatedAt = DateTime.UtcNow,
                Name = SucceededState.StateName,
                Data = JobHelper.ToJson(data),
            }).ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
                context.JobActualStates.AddRange(jobs.Zip(states, (job, state) =>
                    new HangfireJobActualState { Job = job, State = state }));
            });

            var result = UseMonitoringApi(api => api.SucceededJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.All(result, item =>
            {
                Assert.NotNull(item);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(123 + 456, value.TotalDuration);
                Assert.Equal("789", value.Result);
                Assert.Equal(now, value.SucceededAt);
            });
            Assert.Equal(jobs[1].Id.ToString(), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(), result[1].Key);
        }

        [Fact, CleanDatabase]
        public void ProcessingJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                { "StartedAt",  JobHelper.SerializeDateTime(now) },
                { "ServerId", "ServerId" },
                { "ServerName", "ServerName" },
            };

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    InvocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test")),
                }).
                ToArray();
            var states = jobs.Select(x => new HangfireJobState
            {
                Id = Guid.NewGuid(),
                Job = x,
                CreatedAt = DateTime.UtcNow,
                Name = ProcessingState.StateName,
                Data = JobHelper.ToJson(data),
            }).ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
                context.JobActualStates.AddRange(jobs.Zip(states, (job, state) =>
                    new HangfireJobActualState { Job = job, State = state }));
            });

            var result = UseMonitoringApi(api => api.ProcessingJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.All(result, item =>
            {
                Assert.NotNull(item);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal("ServerId", value.ServerId);
                Assert.Equal(now, value.StartedAt);
            });
            Assert.Equal(jobs[1].Id.ToString(), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(), result[1].Key);
        }

        [Fact, CleanDatabase]
        public void ScheduledJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                { "EnqueueAt",  JobHelper.SerializeDateTime(now) },
                { "ScheduledAt",  JobHelper.SerializeDateTime(now.AddSeconds(1)) },
            };

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    InvocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test")),
                }).
                ToArray();
            var states = jobs.Select(x => new HangfireJobState
            {
                Id = Guid.NewGuid(),
                Job = x,
                CreatedAt = DateTime.UtcNow,
                Name = ScheduledState.StateName,
                Data = JobHelper.ToJson(data),
            }).ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
                context.JobActualStates.AddRange(jobs.Zip(states, (job, state) =>
                    new HangfireJobActualState { Job = job, State = state }));
            });

            var result = UseMonitoringApi(api => api.ScheduledJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.All(result, item =>
            {
                Assert.NotNull(item);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(now, value.EnqueueAt);
                Assert.Equal(now.AddSeconds(1), value.ScheduledAt);
            });
            Assert.Equal(jobs[1].Id.ToString(), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(), result[1].Key);
        }

        [Fact, CleanDatabase]
        public void FailedJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                { "FailedAt",  JobHelper.SerializeDateTime(now) },
                { "ExceptionDetails",  "ExceptionDetails" },
                { "ExceptionMessage",  "ExceptionMessage" },
                { "ExceptionType",  "ExceptionType" },
            };

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    InvocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test")),
                }).
                ToArray();
            var states = jobs.Select(x => new HangfireJobState
            {
                Id = Guid.NewGuid(),
                Job = x,
                CreatedAt = DateTime.UtcNow,
                Name = FailedState.StateName,
                Data = JobHelper.ToJson(data),
                Reason = "Reason",
            }).ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
                context.JobActualStates.AddRange(jobs.Zip(states, (job, state) =>
                    new HangfireJobActualState { Job = job, State = state }));
            });

            var result = UseMonitoringApi(api => api.FailedJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.All(result, item =>
            {
                Assert.NotNull(item);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(now, value.FailedAt);
                Assert.Equal("ExceptionDetails", value.ExceptionDetails);
                Assert.Equal("ExceptionMessage", value.ExceptionMessage);
                Assert.Equal("ExceptionType", value.ExceptionType);
                Assert.Equal("Reason", value.Reason);
            });
            Assert.Equal(jobs[1].Id.ToString(), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(), result[1].Key);
        }

        [Fact, CleanDatabase]
        public void DeletedJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                { "DeletedAt",  JobHelper.SerializeDateTime(now) },
                { "ExceptionDetails",  "ExceptionDetails" },
                { "ExceptionMessage",  "ExceptionMessage" },
                { "ExceptionType",  "ExceptionType" },
            };

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    InvocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test")),
                }).
                ToArray();
            var states = jobs.Select(x => new HangfireJobState
            {
                Id = Guid.NewGuid(),
                Job = x,
                CreatedAt = DateTime.UtcNow,
                Name = DeletedState.StateName,
                Data = JobHelper.ToJson(data),
            }).ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
                context.JobActualStates.AddRange(jobs.Zip(states, (job, state) =>
                    new HangfireJobActualState { Job = job, State = state }));
            });

            var result = UseMonitoringApi(api => api.DeletedJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.All(result, item =>
            {
                Assert.NotNull(item);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(now, value.DeletedAt);
            });
            Assert.Equal(jobs[1].Id.ToString(), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(), result[1].Key);
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                { "EnqueuedAt",  JobHelper.SerializeDateTime(now) },
            };

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    Id = Guid.NewGuid(),
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    InvocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test")),
                }).
                ToArray();
            var states = jobs.Select(x => new HangfireJobState
            {
                Id = Guid.NewGuid(),
                Job = x,
                CreatedAt = DateTime.UtcNow,
                Name = EnqueuedState.StateName,
                Data = JobHelper.ToJson(data),
            }).ToArray();
            var queueItems = Enumerable.Range(0, 5).Select(x => new HangfireJobQueueItem
            {
                Id = Guid.NewGuid(),
                Job = jobs[x],
                CreatedAt = DateTime.UtcNow,
                Queue = "queue",
            }).ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
                context.JobActualStates.AddRange(jobs.Zip(states, (job, state) =>
                    new HangfireJobActualState { Job = job, State = state }));
                context.JobQueues.AddRange(queueItems);
            });

            var result = UseMonitoringApi(api => api.EnqueuedJobs("queue", 1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.All(result, item =>
            {
                Assert.NotNull(item);
                var id = Guid.Parse(item.Key);
                Assert.True(jobs.Any(x => x.Id == id));
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(now, value.EnqueuedAt);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsEmptyResult()
        {
            var result = UseMonitoringApi(api => api.FetchedJobs("queue", 0, 2));

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        private Guid AddJobWithStateToContext(HangfireDbContext context, string stateName, string data = null)
        {
            var job = new HangfireJob { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, InvocationData = string.Empty };
            var state = new HangfireJobState { Id = Guid.NewGuid(), Job = job, CreatedAt = DateTime.UtcNow, Name = stateName, Data = data, };
            context.Jobs.Add(job);
            context.JobStates.Add(state);
            context.JobActualStates.Add(new HangfireJobActualState { Job = job, State = state });
            return job.Id;
        }

        private Guid AddJobWithQueueItemToContext(HangfireDbContext context, string stateName, string queue)
        {
            var job = new HangfireJob { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, InvocationData = string.Empty };
            var queueItem = new HangfireJobQueueItem { Id = Guid.NewGuid(), CreatedAt = DateTime.UtcNow, Job = job, Queue = queue, };
            context.Jobs.Add(job);
            context.JobQueues.Add(queueItem);
            return job.Id;
        }

        private T UseMonitoringApi<T>(Func<EntityFrameworkJobStorageMonitoringApi, T> func)
        {
            var storage = CreateStorage();
            return func(new EntityFrameworkJobStorageMonitoringApi(storage));
        }


        [ExcludeFromCodeCoverage]
        public void SampleMethod(string value)
        { }
    }
}
