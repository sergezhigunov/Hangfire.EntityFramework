﻿// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Globalization;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobStorageMonitoringApi : IMonitoringApi
    {
        private const string DeletedCounterName = "stats:deleted";
        private const string SucceededCounterName = "stats:succeeded";
        private const string RecurringJobsSetName = "recurring-jobs";

        private EntityFrameworkJobStorage Storage { get; }

        public EntityFrameworkJobStorageMonitoringApi([NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            Storage = storage;
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var tuples = Storage.QueueProviders
                .Select(x => x.GetJobQueueMonitoringApi())
                .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
                .OrderBy(x => x.Queue)
                .ToArray();

            var result = new List<QueueWithTopEnqueuedJobsDto>(tuples.Length);

            foreach (var tuple in tuples)
            {
                var enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5);
                var enqueuedJobCount = tuple.Monitoring.GetEnqueuedJobCount(tuple.Queue);
                var firstJobs = EnqueuedJobs(enqueuedJobIds);

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = tuple.Queue,
                    Length = enqueuedJobCount,
                    FirstJobs = firstJobs
                });
            }

            return result;
        }

        public IList<ServerDto> Servers()
        {
            var servers = UseContext(context => context.Servers.ToArray());

            return servers.Select(server =>
            {
                string[] queues = JobHelper.FromJson<string[]>(server.Queues);

                return new ServerDto
                {
                    Name = server.Id,
                    Heartbeat = server.Heartbeat,
                    Queues = JobHelper.FromJson<string[]>(server.Queues),
                    StartedAt = server.StartedAt,
                    WorkersCount = server.WorkerCount,
                };
            }).ToList();
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            if (jobId == null)
                return null;

            long id;
            if (!long.TryParse(jobId, NumberStyles.Integer, CultureInfo.InvariantCulture, out id))
                return null;

            return UseContext(context =>
            {
                var job = context.Jobs.
                    Include(x => x.States).
                    Include(x => x.Parameters).
                    SingleOrDefault(x => x.Id == id);

                if (job == null)
                    return null;

                return new JobDetailsDto
                {
                    CreatedAt = job.CreatedAt,
                    ExpireAt = job.ExpireAt,
                    Job = DeserializeJob(job),
                    Properties = job.Parameters.
                        ToDictionary(x => x.Name, x => x.Value),
                    History = job.States.
                        OrderByDescending(x => x.CreatedAt).
                        Select(x => new StateHistoryDto
                        {
                            CreatedAt = x.CreatedAt,
                            Reason = x.Reason,
                            StateName = x.State.ToStateName(),
                            Data = JobHelper.FromJson<Dictionary<string, string>>(x.Data),
                        }).
                        ToList(),
                };
            });
        }

        public StatisticsDto GetStatistics()
        {
            var result = UseContext(context =>
            {
                var stateCounts = (
                    from job in context.Jobs
                    where job.ActualState.HasValue
                    let state = job.ActualState.Value
                    where
                        state == JobState.Enqueued ||
                        state == JobState.Failed ||
                        state == JobState.Processing ||
                        state == JobState.Scheduled
                    group job by state into @group
                    select new
                    {
                        State = @group.Key,
                        Count = @group.LongCount(),
                    }).
                    ToDictionary(x => x.State, x => x.Count);

                var counters = (
                    from counter in context.Counters
                    let name = counter.Key
                    where
                        name == SucceededCounterName ||
                        name == DeletedCounterName
                    group counter by name into @group
                    select new
                    {
                        CounterName = @group.Key,
                        Sum = @group.Sum(x => x.Value),
                    }).
                    ToDictionary(x => x.CounterName, x => x.Sum);

                return new StatisticsDto
                {
                    Recurring = context.Sets.LongCount(x => x.Key == RecurringJobsSetName),
                    Servers = context.Servers.LongCount(),
                    Enqueued = stateCounts.TryGetValue(JobState.Enqueued, out long count) ? count : 0,
                    Failed = stateCounts.TryGetValue(JobState.Failed, out count) ? count : 0,
                    Processing = stateCounts.TryGetValue(JobState.Processing, out count) ? count : 0,
                    Scheduled = stateCounts.TryGetValue(JobState.Scheduled, out count) ? count : 0,
                    Deleted = counters.TryGetValue(DeletedCounterName, out count) ? count : 0,
                    Succeeded = counters.TryGetValue(SucceededCounterName, out count) ? count : 0,
                };
            });

            result.Queues = (
                from provider in Storage.QueueProviders
                from queue in provider.GetJobQueueMonitoringApi().GetQueues()
                select queue).
                Count();

            return result;
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var queueApi = GetQueueApi(queue);
            var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, perPage);
            return EnqueuedJobs(enqueuedJobIds.ToArray());
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage) =>
            new JobList<FetchedJobDto>(Enumerable.Empty<KeyValuePair<string, FetchedJobDto>>());

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobs<ProcessingJobDto, ProcessingStateData>(from, count, JobState.Processing,
                (sqlJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData?.ServerId ?? stateData?.ServerName,
                    StartedAt = stateData?.StartedAt,
                });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobs<ScheduledJobDto, ScheduledStateData>(from, count, JobState.Scheduled,
                (sqlJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = stateData?.EnqueueAt ?? default(DateTime),
                    ScheduledAt = stateData?.ScheduledAt,
                });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobs<SucceededJobDto, SucceededStateData>(from, count, JobState.Succeeded,
                (sqlJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    Result = stateData?.Result,
                    TotalDuration = stateData?.PerformanceDuration + stateData?.Latency,
                    SucceededAt = stateData?.SucceededAt,
                });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobs<FailedJobDto, FailedStateData>(from, count, JobState.Failed,
                (sqlJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = sqlJob.StateReason,
                    ExceptionDetails = stateData?.ExceptionDetails,
                    ExceptionMessage = stateData?.ExceptionMessage,
                    ExceptionType = stateData?.ExceptionType,
                    FailedAt = stateData?.FailedAt,
                });
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobs<DeletedJobDto, DeletedStateData>(from, count, JobState.Deleted,
                (sqlJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = stateData?.DeletedAt,
                });
        }

        public long ScheduledCount() => GetNumberOfJobsByStateName(JobState.Scheduled);

        public long EnqueuedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            return queueApi.GetEnqueuedJobCount(queue);
        }

        public long FetchedCount(string queue) => 0;

        public long FailedCount() => GetNumberOfJobsByStateName(JobState.Failed);

        public long ProcessingCount() => GetNumberOfJobsByStateName(JobState.Processing);

        public long SucceededListCount() => GetNumberOfJobsByStateName(JobState.Succeeded);

        public long DeletedListCount() => GetNumberOfJobsByStateName(JobState.Deleted);

        public IDictionary<DateTime, long> SucceededByDatesCount() =>
            UseContext(context => GetTimelineStats("succeeded"));

        public IDictionary<DateTime, long> FailedByDatesCount() =>
            UseContext(context => GetTimelineStats("failed"));

        public IDictionary<DateTime, long> HourlySucceededJobs() => GetHourlyTimelineStats("succeeded");

        public IDictionary<DateTime, long> HourlyFailedJobs() => GetHourlyTimelineStats("failed");

        private JobList<EnqueuedJobDto> EnqueuedJobs(long[] enqueuedJobIds)
        {
            if (enqueuedJobIds.Length == 0)
                return new JobList<EnqueuedJobDto>(Enumerable.Empty<KeyValuePair<string, EnqueuedJobDto>>());

            return UseContext(context =>
            {
                var jobs = (
                    from job in context.Jobs.
                    WhereContains(x => x.Id, enqueuedJobIds)
                    join actualState in GetActualStates(context)
                        on job.Id equals actualState.JobId
                    orderby job.CreatedAt ascending
                    select new JobInfo
                    {
                        Id = job.Id,
                        State = job.ActualState.Value,
                        ClrType = job.ClrType,
                        Method = job.Method,
                        ArgumentTypes = job.ArgumentTypes,
                        Arguments = job.Arguments,
                        StateData = actualState.Data,
                        StateReason = actualState.Reason,
                    }).
                    ToArray();

                return DeserializeJobs<EnqueuedJobDto, EnqueuedStateData>(
                    jobs,
                    (sqlJob, job, stateData) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = sqlJob.State.ToStateName(),
                        EnqueuedAt = stateData?.EnqueuedAt
                    });
            });
        }

        private JobList<TResult> GetJobs<TResult, TStateData>(
            int from,
            int count,
            JobState state,
            Func<JobInfo, Job, TStateData, TResult> selector)
        {
            return UseContext(context =>
            {
                var jobs = (
                    from job in context.Jobs
                    where job.ActualState.Value == state
                    join actualState in GetActualStates(context)
                        on job.Id equals actualState.JobId
                    orderby job.CreatedAt descending
                    select new JobInfo
                    {
                        Id = job.Id,
                        State = job.ActualState.Value,
                        ClrType = job.ClrType,
                        Method = job.Method,
                        ArgumentTypes = job.ArgumentTypes,
                        Arguments = job.Arguments,
                        StateData = actualState.Data,
                        StateReason = actualState.Reason,
                    }).
                    Skip(() => from).
                    Take(() => count).
                    ToArray();

                return DeserializeJobs(jobs, selector);
            });
        }

        private JobList<TResult> DeserializeJobs<TResult, TStateData>(
            JobInfo[] jobs,
            Func<JobInfo, Job, TStateData, TResult> selector)
        {
            var result = new List<KeyValuePair<string, TResult>>(jobs.Length);

            foreach (var job in jobs)
            {
                var dto = default(TResult);

                if (!string.IsNullOrWhiteSpace(job.ClrType) && !string.IsNullOrWhiteSpace(job.Method))
                {
                    TStateData stateData = JobHelper.FromJson<TStateData>(job.StateData);
                    dto = selector(job, DeserializeJob(job), stateData);
                }

                result.Add(new KeyValuePair<string, TResult>(job.Id.ToString(CultureInfo.InvariantCulture), dto));
            }

            return new JobList<TResult>(result);
        }

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queue)
        {
            var provider = Storage.QueueProviders.GetProvider(queue);
            return provider.GetJobQueueMonitoringApi();
        }

        private long GetNumberOfJobsByStateName(JobState state)
        {
            return UseContext(context => (
                from job in context.Jobs
                where job.ActualState == state
                select job).
                LongCount());
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            var ticks = DateTime.UtcNow.Ticks;
            var endDate = new DateTime(ticks - ticks % TimeSpan.TicksPerHour, DateTimeKind.Utc);
            var dates = Enumerable.Range(0, 24).Select(x => endDate.AddHours(-x));
            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd-HH}");

            return GetTimelineStats(keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var dates = Enumerable.Range(0, 7).Select(x => endDate.AddDays(-x));
            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd}");

            return GetTimelineStats(keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(IDictionary<string, DateTime> keyMaps)
        {
            var valuesMap = UseContext(context => (
                from counter in context.Counters.
                WhereContains(x => x.Key, keyMaps.Keys)
                group counter by counter.Key into groupByKey
                select new
                {
                    groupByKey.Key,
                    Count = groupByKey.Sum(x => x.Value),
                }).
                ToDictionary(x => x.Key, x => x.Count));

            foreach (var key in keyMaps.Keys)
                if (!valuesMap.ContainsKey(key))
                    valuesMap.Add(key, 0);

            return keyMaps.ToDictionary(x => x.Value, x => valuesMap[x.Key]);
        }

        private T UseContext<T>(Func<HangfireDbContext, T> func) => Storage.UseContext(func);

        private static IQueryable<HangfireJobState> GetActualStates(HangfireDbContext context)
        {
            var states = context.JobStates.Where(x => x.State == x.Job.ActualState);

            return
                from actualState in states
                join state in (
                    from state in states
                    group state by state.JobId into grouping
                    select new
                    {
                        JobId = grouping.Key,
                        CreatedAt = grouping.Max(x => x.CreatedAt),
                    })
                    on new
                    {
                        actualState.JobId,
                        actualState.CreatedAt,
                    }
                    equals new
                    {
                        state.JobId,
                        state.CreatedAt,
                    }
                select actualState;
        }

        private static Job DeserializeJob(JobInfo job)
        {
            var data = new InvocationData(
                job.ClrType,
                job.Method,
                job.ArgumentTypes,
                job.Arguments);

            return Deserialize(data);
        }

        private static Job DeserializeJob(HangfireJob job)
        {
            var data = new InvocationData(
                job.ClrType,
                job.Method,
                job.ArgumentTypes,
                job.Arguments);

            return Deserialize(data);
        }

        private static Job Deserialize(InvocationData data)
        {
            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }
    }
}