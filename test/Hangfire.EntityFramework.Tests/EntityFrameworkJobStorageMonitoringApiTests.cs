// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using Hangfire.Common;
using Hangfire.EntityFramework.Utils;
using Hangfire.States;
using Moq;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    [CleanDatabase]
    public class EntityFrameworkJobStorageMonitoringApiTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobStorageMonitoringApi(null));
        }

        [Fact, RollbackTransaction]
        public void Queues_ReturnsEmptyList_WhenNoQueuesExists()
        {
            var result = UseMonitoringApi(api => api.Queues());

            Assert.NotNull(result);
            Assert.Equal(0, result.Count);
        }

        [Fact, RollbackTransaction]
        public void Queues_ReturnsCorrectList()
        {
            var invocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test"));

            var job = new HangfireJob
            {
                CreatedAt = DateTime.UtcNow,
                ClrType = invocationData.Type,
                Method = invocationData.Method,
                ArgumentTypes = invocationData.ParameterTypes,
                Arguments = invocationData.Arguments,
                ActualState = JobState.Awaiting,
            };

            var state = new HangfireJobState
            {
                Job = job,
                CreatedAt = DateTime.UtcNow,
                State = JobState.Awaiting,
            };

            var jobQueueItem = new HangfireJobQueue
            {
                Job = job,
                Queue = "DEFAULT",
            };

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.Add(job);
                context.JobStates.Add(state);
                context.JobQueues.Add(jobQueueItem);
            });

            var result = UseMonitoringApi(api => api.Queues());

            Assert.NotNull(result);
            Assert.Equal(1, result.Count);
            var firstItem = result.First();
            Assert.Equal("DEFAULT", firstItem.Name);
            Assert.Null(firstItem.Fetched);
            Assert.Equal(1, firstItem.Length);
            Assert.Single(firstItem.FirstJobs);
            var firstJobKeyValuePair = firstItem.FirstJobs.First();
            Assert.Equal(job.Id.ToString(CultureInfo.InvariantCulture), firstJobKeyValuePair.Key);
            var firstJob = firstJobKeyValuePair.Value;
            Assert.Null(firstJob.EnqueuedAt);
            Assert.True(firstJob.InEnqueuedState);
            Assert.Equal(AwaitingState.StateName, firstJob.State);
            var firstJobInvocationData = JobUtils.CreateInvocationData(firstJob.Job);
            Assert.Equal(job.ClrType, firstJobInvocationData.Type);
            Assert.Equal(job.Method, firstJobInvocationData.Method);
            Assert.Equal(job.ArgumentTypes, firstJobInvocationData.ParameterTypes);
            Assert.Equal(job.Arguments, firstJobInvocationData.Arguments);
        }

        [Fact, RollbackTransaction]
        public void Servers_ReturnsEmptyList_WhenNoServersExists()
        {
            var result = UseMonitoringApi(api => api.Servers());

            Assert.NotNull(result);
            Assert.Equal(0, result.Count);
        }

        [Fact, RollbackTransaction]
        public void Servers_ReturnsCorrectList()
        {
            string serverId1 = "server1";
            string serverId2 = "server2";
            var workerCount = 4;
            var startedAt1 = new DateTime(2017, 1, 1, 11, 22, 33, DateTimeKind.Utc);
            var startedAt2 = new DateTime(2017, 1, 1, 22, 33, 44, DateTimeKind.Utc);
            var heartbeat = new DateTime(2017, 3, 3, 23, 34, 45, DateTimeKind.Utc);
            var queues = new[]
            {
                "CRITICAL",
                "DEFAULT",
            };

            var queuesJson = JobHelper.ToJson(queues);

            var host = new HangfireServerHost
            {
                Id = EntityFrameworkJobStorage.ServerHostId
            };
            var servers = new[]
            {
                new HangfireServer
                {
                    Id = serverId1,
                    StartedAt = startedAt1,
                    Heartbeat = heartbeat,
                    WorkerCount = workerCount,
                    Queues = queuesJson,
                    ServerHost = host,
                },
                new HangfireServer
                {
                    Id = serverId2,
                    StartedAt = startedAt2,
                    Heartbeat = heartbeat,
                    ServerHost = host,
                },
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
            Assert.Equal(startedAt1, server1.StartedAt);
            Assert.Equal(heartbeat, server2.Heartbeat);
            Assert.Equal(0, server2.WorkersCount);
            Assert.False(server2.Queues?.Any() == true);
            Assert.Equal(startedAt2, server2.StartedAt);
        }

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
        public void GetStatistics_ReturnsCorrectCounts()
        {
            var startedAt = new DateTime(2017, 1, 1, 11, 33, 33);

            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 1; i++)
                    AddJobWithStateToContext(context, JobState.Enqueued);

                for (int i = 0; i < 2; i++)
                    AddJobWithStateToContext(context, JobState.Failed);

                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, JobState.Processing);

                for (int i = 0; i < 4; i++)
                    AddJobWithStateToContext(context, JobState.Scheduled);

                context.Counters.Add(new HangfireCounter
                {
                    Id = Guid.NewGuid(),
                    Key = "stats:deleted",
                    Value = 5,
                });

                context.Counters.Add(new HangfireCounter
                {
                    Id = Guid.NewGuid(),
                    Key = "stats:succeeded",
                    Value = 6,
                });

                for (int i = 0; i < 7; i++)
                    context.Sets.Add(new HangfireSet
                    {
                        Key = "recurring-jobs",
                        Value = Guid.NewGuid().ToString(),
                        CreatedAt = DateTime.UtcNow,
                    });

                var host = context.ServerHosts.Add(new HangfireServerHost
                {
                    Id = EntityFrameworkJobStorage.ServerHostId,
                });

                for (int i = 0; i < 8; i++)
                    context.Servers.Add(new HangfireServer
                    {
                        Id = Guid.NewGuid().ToString(),
                        StartedAt = startedAt,
                        Heartbeat = DateTime.UtcNow,
                        ServerHostId = host.Id,
                    });

                for (int i = 0; i < 9; i++)
                    AddJobWithQueueItemToContext(context, Guid.NewGuid().ToString());
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

        [Theory, RollbackTransaction]
        [InlineData(null)]
        [InlineData("1")]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        public void JobDetails_ReturnsNull_WhenJobNotExists(string jobId)
        {
            var result = UseMonitoringApi(api => api.JobDetails(jobId));

            Assert.Null(result);
        }

        [Fact, RollbackTransaction]
        public void JobDetails_ReturnsCorrectResult()
        {
            var timestamp = DateTime.UtcNow;
            var createdAt = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Unspecified);
            var stateData = new Dictionary<string, string>
            {
                ["Name"] = "Value",
            };

            var state = new Mock<IState>();
            state.Setup(x => x.Name).Returns(AwaitingState.StateName);
            state.Setup(x => x.Reason).Returns("Reason");
            state.Setup(x => x.SerializeData()).Returns(stateData);

            var jobParameters = new Dictionary<string, string>
            {
                ["Key1"] = "Value1",
                ["Key2"] = "Value2",
            };

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
            Assert.Equal(AwaitingState.StateName, historyItem.StateName);
            Assert.Equal("Reason", historyItem.Reason);
            Assert.True(historyItem.CreatedAt >= timestamp);
            Assert.Equal(stateData, historyItem.Data);
            Assert.NotNull(result.Properties);
            Assert.Equal(jobParameters, result.Properties);
        }

        [Fact, RollbackTransaction]
        public void SucceededByDatesCount_ReturnsCorrectResult()
        {
            var today = DateTime.UtcNow.Date;
            var counts = Enumerable.Range(0, 7);
            var dictionaryDates = counts.ToDictionary(x => today.AddDays(-x));

            UseContextWithSavingChanges(context =>
            {
                foreach (var item in dictionaryDates)
                    if (item.Value != 0)
                        context.Counters.Add(new HangfireCounter
                        {
                            Id = Guid.NewGuid(),
                            Key = $"stats:succeeded:{item.Key:yyyy-MM-dd}",
                            Value = item.Value,
                        });
            });

            var result = UseMonitoringApi(api => api.SucceededByDatesCount());

            Assert.NotNull(result);
            Assert.Equal(7, result.Count);
            Assert.All(result, item => Assert.Equal(dictionaryDates[item.Key], item.Value));
        }

        [Fact, RollbackTransaction]
        public void FailedByDatesCount_ReturnsCorrectResult()
        {
            var today = DateTime.UtcNow.Date;
            var counts = Enumerable.Range(0, 7);
            var dictionaryDates = counts.ToDictionary(x => today.AddDays(-x));

            UseContextWithSavingChanges(context =>
            {
                foreach (var item in dictionaryDates)
                    if (item.Value != 0)
                        context.Counters.Add(new HangfireCounter
                        {
                            Id = Guid.NewGuid(),
                            Key = $"stats:failed:{item.Key:yyyy-MM-dd}",
                            Value = item.Value,
                        });
            });

            var result = UseMonitoringApi(api => api.FailedByDatesCount());

            Assert.NotNull(result);
            Assert.Equal(7, result.Count);
            Assert.All(result, item => Assert.Equal(dictionaryDates[item.Key], item.Value));
        }

        [Fact, RollbackTransaction]
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
                        context.Counters.Add(new HangfireCounter
                        {
                            Id = Guid.NewGuid(),
                            Key = $"stats:succeeded:{item.Key:yyyy-MM-dd-HH}",
                            Value = item.Value,
                        });
            });

            var result = UseMonitoringApi(api => api.HourlySucceededJobs());

            Assert.NotNull(result);
            Assert.Equal(24, result.Count);
            Assert.All(result, item => Assert.Equal(dictionaryDates[item.Key], item.Value));
        }

        [Fact, RollbackTransaction]
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
                        context.Counters.Add(new HangfireCounter
                        {
                            Id = Guid.NewGuid(),
                            Key = $"stats:failed:{item.Key:yyyy-MM-dd-HH}",
                            Value = item.Value,
                        });
            });

            var result = UseMonitoringApi(api => api.HourlyFailedJobs());

            Assert.NotNull(result);
            Assert.Equal(24, result.Count);

            Assert.All(result, item => Assert.Equal(dictionaryDates[item.Key], item.Value));
        }

        [Fact, RollbackTransaction]
        public void ScheduledCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, JobState.Scheduled);
            });

            var result = UseMonitoringApi(api => api.ScheduledCount());

            Assert.Equal(3, result);
        }

        [Fact, RollbackTransaction]
        public void FailedCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, JobState.Failed);
            });

            var result = UseMonitoringApi(api => api.FailedCount());

            Assert.Equal(3, result);
        }

        [Fact, RollbackTransaction]
        public void ProcessingCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, JobState.Processing);
            });

            var result = UseMonitoringApi(api => api.ProcessingCount());

            Assert.Equal(3, result);
        }

        [Fact, RollbackTransaction]
        public void SucceededListCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, JobState.Succeeded);
            });

            var result = UseMonitoringApi(api => api.SucceededListCount());

            Assert.Equal(3, result);
        }

        [Fact, RollbackTransaction]
        public void DeletedListCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithStateToContext(context, JobState.Deleted);
            });

            var result = UseMonitoringApi(api => api.DeletedListCount());

            Assert.Equal(3, result);
        }

        [Fact, RollbackTransaction]
        public void EnqueuedCount_ReturnsCorrectResult()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 3; i++)
                    AddJobWithQueueItemToContext(context, "QUEUE");
            });

            var result = UseMonitoringApi(api => api.EnqueuedCount("QUEUE"));

            Assert.Equal(3, result);
        }

        [Fact, RollbackTransaction]
        public void FetchedCount_ReturnsZero()
        {
            var result = UseMonitoringApi(api => api.FetchedCount("QUEUE"));

            Assert.Equal(0, result);
        }

        [Fact, RollbackTransaction]
        public void SucceededJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                ["SucceededAt"] = JobHelper.SerializeDateTime(now),
                ["PerformanceDuration"] = "123",
                ["Latency"] = "456",
                ["Result"] = "789",
            };

            var invocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test"));

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    ClrType = invocationData.Type,
                    Method = invocationData.Method,
                    ArgumentTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                    ActualState = JobState.Succeeded,
                }).
                ToArray();

            var states = jobs.Select(x => new HangfireJobState
            {
                Job = x,
                CreatedAt = DateTime.UtcNow,
                State = JobState.Succeeded,
                Data = JobHelper.ToJson(data),
            }).
            ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
            });

            var result = UseMonitoringApi(api => api.SucceededJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);

            Assert.All(result, item =>
            {
                Assert.NotNull(item.Key);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(123 + 456, value.TotalDuration);
                Assert.Equal("789", value.Result);
                Assert.Equal(now, value.SucceededAt);
            });

            Assert.Equal(jobs[1].Id.ToString(CultureInfo.InvariantCulture), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(CultureInfo.InvariantCulture), result[1].Key);
        }

        [Fact, RollbackTransaction]
        public void ProcessingJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                ["StartedAt"] = JobHelper.SerializeDateTime(now),
                ["ServerId"] = "ServerId",
                ["ServerName"] = "ServerName",
            };

            var invocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test"));

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    ClrType = invocationData.Type,
                    Method = invocationData.Method,
                    ArgumentTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                    ActualState = JobState.Processing,
                }).
                ToArray();

            var states = jobs.Select(x => new HangfireJobState
            {
                Job = x,
                CreatedAt = DateTime.UtcNow,
                State = JobState.Processing,
                Data = JobHelper.ToJson(data),
            }).
            ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
            });

            var result = UseMonitoringApi(api => api.ProcessingJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);

            Assert.All(result, item =>
            {
                Assert.NotNull(item.Key);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal("ServerId", value.ServerId);
                Assert.Equal(now, value.StartedAt);
            });

            Assert.Equal(jobs[1].Id.ToString(CultureInfo.InvariantCulture), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(CultureInfo.InvariantCulture), result[1].Key);
        }

        [Fact, RollbackTransaction]
        public void ScheduledJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                ["EnqueueAt"] = JobHelper.SerializeDateTime(now),
                ["ScheduledAt"] = JobHelper.SerializeDateTime(now.AddSeconds(1)),
            };

            var invocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test"));

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    ClrType = invocationData.Type,
                    Method = invocationData.Method,
                    ArgumentTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                    ActualState = JobState.Scheduled,
                }).
                ToArray();

            var states = jobs.Select(x => new HangfireJobState
            {
                Job = x,
                CreatedAt = DateTime.UtcNow,
                State = JobState.Scheduled,
                Data = JobHelper.ToJson(data),
            }).
            ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
            });

            var result = UseMonitoringApi(api => api.ScheduledJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);

            Assert.All(result, item =>
            {
                Assert.NotNull(item.Key);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(now, value.EnqueueAt);
                Assert.Equal(now.AddSeconds(1), value.ScheduledAt);
            });

            Assert.Equal(jobs[1].Id.ToString(CultureInfo.InvariantCulture), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(CultureInfo.InvariantCulture), result[1].Key);
        }

        [Fact, RollbackTransaction]
        public void FailedJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                ["FailedAt"] = JobHelper.SerializeDateTime(now),
                ["ExceptionDetails"] = "ExceptionDetails",
                ["ExceptionMessage"] = "ExceptionMessage",
                ["ExceptionType"] = "ExceptionType",
            };

            var invocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test"));

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    ClrType = invocationData.Type,
                    Method = invocationData.Method,
                    ArgumentTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                    ActualState = JobState.Failed,
                }).
                ToArray();

            var states = jobs.Select(x => new HangfireJobState
            {
                Job = x,
                CreatedAt = DateTime.UtcNow,
                State = JobState.Failed,
                Data = JobHelper.ToJson(data),
                Reason = "Reason",
            }).ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
            });

            var result = UseMonitoringApi(api => api.FailedJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.All(result, item =>
            {
                Assert.NotNull(item.Key);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(now, value.FailedAt);
                Assert.Equal("ExceptionDetails", value.ExceptionDetails);
                Assert.Equal("ExceptionMessage", value.ExceptionMessage);
                Assert.Equal("ExceptionType", value.ExceptionType);
                Assert.Equal("Reason", value.Reason);
            });
            Assert.Equal(jobs[1].Id.ToString(CultureInfo.InvariantCulture), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(CultureInfo.InvariantCulture), result[1].Key);
        }

        [Fact, RollbackTransaction]
        public void DeletedJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                ["DeletedAt"] = JobHelper.SerializeDateTime(now),
                ["ExceptionDetails"] = "ExceptionDetails",
                ["ExceptionMessage"] = "ExceptionMessage",
                ["ExceptionType"] = "ExceptionType",
            };

            var invocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test"));

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    ClrType = invocationData.Type,
                    Method = invocationData.Method,
                    ArgumentTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                    ActualState = JobState.Deleted,
                }).
                ToArray();

            var states = jobs.Select(x => new HangfireJobState
            {
                Job = x,
                CreatedAt = DateTime.UtcNow,
                State = JobState.Deleted,
                Data = JobHelper.ToJson(data),
            }).
            ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
            });

            var result = UseMonitoringApi(api => api.DeletedJobs(1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);

            Assert.All(result, item =>
            {
                Assert.NotNull(item.Key);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(now, value.DeletedAt);
            });

            Assert.Equal(jobs[1].Id.ToString(CultureInfo.InvariantCulture), result[0].Key);
            Assert.Equal(jobs[2].Id.ToString(CultureInfo.InvariantCulture), result[1].Key);
        }

        [Fact, RollbackTransaction]
        public void EnqueuedJobs_ReturnsCorrectResult()
        {
            var now = DateTime.UtcNow;

            var data = new Dictionary<string, string>
            {
                ["EnqueuedAt"] = JobHelper.SerializeDateTime(now),
            };

            var invocationData = JobUtils.CreateInvocationData(() => SampleMethod("Test"));

            var jobs = Enumerable.Range(0, 5).Select(x =>
                new HangfireJob
                {
                    CreatedAt = now - new TimeSpan(0, 0, x),
                    ClrType = invocationData.Type,
                    Method = invocationData.Method,
                    ArgumentTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                    ActualState = JobState.Enqueued,
                }).
                ToArray();

            var states = jobs.Select(x => new HangfireJobState
            {
                Job = x,
                CreatedAt = DateTime.UtcNow,
                State = JobState.Enqueued,
                Data = JobHelper.ToJson(data),
            }).
            ToArray();

            var queueItems = Enumerable.Range(0, 5).Select(x => new HangfireJobQueue
            {
                Job = jobs[x],
                Queue = "QUEUE",
            }).
            ToArray();

            UseContextWithSavingChanges(context =>
            {
                context.Jobs.AddRange(jobs);
                context.JobStates.AddRange(states);
                context.JobQueues.AddRange(queueItems);
            });

            var result = UseMonitoringApi(api => api.EnqueuedJobs("QUEUE", 1, 2));

            Assert.NotNull(result);
            Assert.Equal(2, result.Count);

            Assert.All(result, item =>
            {
                Assert.NotNull(item.Key);
                var id = long.Parse(item.Key, CultureInfo.InvariantCulture);
                Assert.Contains(jobs, x => x.Id == id);
                var value = item.Value;
                Assert.NotNull(value);
                Assert.Equal(now, value.EnqueuedAt);
            });
        }

        [Fact, RollbackTransaction]
        public void FetchedJobs_ReturnsEmptyResult()
        {
            var result = UseMonitoringApi(api => api.FetchedJobs("QUEUE", 0, 2));

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        private void AddJobWithStateToContext(HangfireDbContext context, JobState jobState, string data = null)
        {
            var job = new HangfireJob
            {
                CreatedAt = DateTime.UtcNow,
                ActualState = jobState,
            };

            var state = new HangfireJobState
            {
                Job = job,
                CreatedAt = DateTime.UtcNow,
                State = jobState,
                Data = data,
            };

            context.Jobs.Add(job);
            context.JobStates.Add(state);
        }

        private void AddJobWithQueueItemToContext(HangfireDbContext context, string queue)
        {
            var job = new HangfireJob
            {
                CreatedAt = DateTime.UtcNow,
            };
            var queueItem = new HangfireJobQueue
            {
                Job = job,
                Queue = queue,
            };
            context.Jobs.Add(job);
            context.JobQueues.Add(queueItem);
        }

        private T UseMonitoringApi<T>(Func<EntityFrameworkJobStorageMonitoringApi, T> func)
        {
            var storage = CreateStorage();
            return func(new EntityFrameworkJobStorageMonitoringApi(storage));
        }


        [ExcludeFromCodeCoverage]
        [SuppressMessage("Usage", "xUnit1013")]
        public void SampleMethod(string value)
        {
            Debug.WriteLine("SampleMethod executed. value = '{0}'", new[] { value });
        }
    }
}
