// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Data.Entity;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobStorageMonitoringApi : IMonitoringApi
    {
        private EntityFrameworkJobStorage Storage { get; }

        public EntityFrameworkJobStorageMonitoringApi([NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

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
            var servers = UseHangfireDbContext(context => context.Servers.ToArray());

            return servers.Select(server =>
            {
                ServerData data = JobHelper.FromJson<ServerData>(server.Data);

                return new ServerDto
                {
                    Name = server.Id,
                    Heartbeat = server.Heartbeat,
                    Queues = data?.Queues,
                    StartedAt = data?.StartedAt ?? DateTime.MinValue,
                    WorkersCount = data?.WorkerCount ?? 0,
                };
            }).ToList();
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            if (jobId == null) return null;

            Guid id;
            if (!Guid.TryParse(jobId, out id))
                return null;

            return UseHangfireDbContext(context =>
            {
                var job = context.Jobs.
                    Include(x => x.States).
                    Include(x => x.Parameters).
                    SingleOrDefault(x => x.Id == id);

                if (job == null) return null;

                return new JobDetailsDto
                {
                    CreatedAt = job.CreatedAt,
                    ExpireAt = job.ExpireAt,
                    Job = DeserializeJob(job.InvocationData),
                    Properties = job.Parameters.
                        ToDictionary(x => x.Name, x => x.Value),
                    History = job.States.
                        OrderByDescending(x => x.CreatedAt).
                        Select(x => new StateHistoryDto
                        {
                            CreatedAt = x.CreatedAt,
                            Reason = x.Reason,
                            StateName = x.Name,
                            Data = JobHelper.FromJson<Dictionary<string, string>>(x.Data),
                        }).
                        ToList(),
                };
            });
        }

        public StatisticsDto GetStatistics()
        {
            Dictionary<string, long> statisticsDictionary = null;

            var statistics = UseHangfireDbContext(context =>
            {
                statisticsDictionary = (
                    from actualState in context.JobActualStates
                    let name = actualState.State.Name
                    group actualState by name into @group
                    select new
                    {
                        @group.Key,
                        Count = @group.LongCount()
                    }).
                    ToArray().
                    ToDictionary(x => x.Key, x => x.Count);

                var counterStatistics = (
                    from counter in context.Counters
                    where counter.Key == "stats:succeeded" || counter.Key == "stats:deleted"
                    group counter by counter.Key into @group
                    select new
                    {
                        @group.Key,
                        Count = @group.Sum(x => x.Value),
                    }).
                    ToArray();

                foreach (var counter in counterStatistics)
                    statisticsDictionary[counter.Key] = counter.Count;

                return new StatisticsDto
                {
                    Servers = context.Servers.LongCount(),
                    Recurring = context.Sets.LongCount(x => x.Key == "recurring-jobs"),
                };
            });

            long count;

            if (statisticsDictionary.TryGetValue(EnqueuedState.StateName, out count))
                statistics.Enqueued = count;
            if (statisticsDictionary.TryGetValue(FailedState.StateName, out count))
                statistics.Failed = count;
            if (statisticsDictionary.TryGetValue(ProcessingState.StateName, out count))
                statistics.Processing = count;
            if (statisticsDictionary.TryGetValue(ScheduledState.StateName, out count))
                statistics.Scheduled = count;
            if (statisticsDictionary.TryGetValue("stats:succeeded", out count))
                statistics.Succeeded = count;
            if (statisticsDictionary.TryGetValue("stats:deleted", out count))
                statistics.Deleted = count;

            statistics.Queues = Storage.QueueProviders
                .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                .Count();

            return statistics;
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
            return GetJobs<ProcessingJobDto, ProcessingStateData>(from, count, ProcessingState.StateName,
                (sqlJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData?.ServerId ?? stateData?.ServerName,
                    StartedAt = stateData?.StartedAt,
                });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobs<ScheduledJobDto, ScheduledStateData>(from, count, ScheduledState.StateName,
                (sqlJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = stateData?.EnqueueAt ?? default(DateTime),
                    ScheduledAt = stateData?.ScheduledAt,
                });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobs<SucceededJobDto, SucceededStateData>(from, count, SucceededState.StateName,
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
            return GetJobs<FailedJobDto, FailedStateData>(from, count, FailedState.StateName,
                (sqlJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = sqlJob.ActualState.State.Reason,
                    ExceptionDetails = stateData?.ExceptionDetails,
                    ExceptionMessage = stateData?.ExceptionMessage,
                    ExceptionType = stateData?.ExceptionType,
                    FailedAt = stateData?.FailedAt,
                });
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobs<DeletedJobDto, DeletedStateData>(from, count, DeletedState.StateName,
                (sqlJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = stateData?.DeletedAt,
                });
        }

        public long ScheduledCount() => GetNumberOfJobsByStateName(ScheduledState.StateName);

        public long EnqueuedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            return queueApi.GetEnqueuedJobCount(queue);
        }

        public long FetchedCount(string queue) => 0;

        public long FailedCount() => GetNumberOfJobsByStateName(FailedState.StateName);

        public long ProcessingCount() => GetNumberOfJobsByStateName(ProcessingState.StateName);

        public long SucceededListCount() => GetNumberOfJobsByStateName(SucceededState.StateName);

        public long DeletedListCount() => GetNumberOfJobsByStateName(DeletedState.StateName);

        public IDictionary<DateTime, long> SucceededByDatesCount() =>
            UseHangfireDbContext(context => GetTimelineStats("succeeded"));

        public IDictionary<DateTime, long> FailedByDatesCount() =>
            UseHangfireDbContext(context => GetTimelineStats("failed"));

        public IDictionary<DateTime, long> HourlySucceededJobs() => GetHourlyTimelineStats("succeeded");

        public IDictionary<DateTime, long> HourlyFailedJobs() => GetHourlyTimelineStats("failed");

        private JobList<EnqueuedJobDto> EnqueuedJobs(Guid[] enqueuedJobIds)
        {
            return UseHangfireDbContext(context =>
            {
                var jobs = (
                    from job in context.Jobs.
                    Include(x => x.ActualState.State)
                    where enqueuedJobIds.Contains(job.Id)
                    orderby job.CreatedAt ascending
                    select job).
                    ToArray();

                return DeserializeJobs<EnqueuedJobDto, EnqueuedStateData>(
                    jobs,
                    (sqlJob, job, stateData) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = sqlJob.ActualState.State.Name,
                        EnqueuedAt = stateData?.EnqueuedAt
                    });
            });
        }

        private JobList<TResult> GetJobs<TResult, TStateData>(
            int from,
            int count,
            string stateName,
            Func<HangfireJob, Job, TStateData, TResult> selector)
        {
            return UseHangfireDbContext(context =>
            {
                var jobs = (
                    from job in context.Jobs.
                    Include(x => x.ActualState.State)
                    where job.ActualState.State.Name == stateName
                    orderby job.CreatedAt descending
                    select job).
                    Skip(() => from).
                    Take(() => count).
                    ToArray();

                return DeserializeJobs(jobs, selector);
            });
        }

        private JobList<TResult> DeserializeJobs<TResult, TStateData>(HangfireJob[] jobs, Func<HangfireJob, Job, TStateData, TResult> selector)
        {
            var result = new List<KeyValuePair<string, TResult>>(jobs.Length);

            foreach (var job in jobs)
            {
                var dto = default(TResult);

                if (!string.IsNullOrWhiteSpace(job.InvocationData))
                {
                    TStateData stateData = JobHelper.FromJson<TStateData>(job.ActualState.State.Data);
                    dto = selector(job, DeserializeJob(job.InvocationData), stateData);
                }

                result.Add(new KeyValuePair<string, TResult>(job.Id.ToString(), dto));
            }

            return new JobList<TResult>(result);
        }

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queue)
        {
            var provider = Storage.QueueProviders.GetProvider(queue);
            return provider.GetJobQueueMonitoringApi();
        }

        private long GetNumberOfJobsByStateName(string stateName)
        {
            return UseHangfireDbContext(context => (
                from actualState in context.JobActualStates
                where actualState.State.Name == stateName
                select actualState).
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
            var valuesMap = UseHangfireDbContext(context => (
                from counter in context.Counters
                where keyMaps.Keys.Contains(counter.Key)
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

        private T UseHangfireDbContext<T>(Func<HangfireDbContext, T> func) => Storage.UseContext(func);

        private static Job DeserializeJob(string invocationData)
        {
            var data = JobHelper.FromJson<InvocationData>(invocationData);

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