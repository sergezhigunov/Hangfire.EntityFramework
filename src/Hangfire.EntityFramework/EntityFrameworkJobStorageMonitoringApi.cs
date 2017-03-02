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
                var counters = tuple.Monitoring.GetJobQueueCounters(tuple.Queue);

                var firstJobs = EnqueuedJobs(enqueuedJobIds);

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = tuple.Queue,
                    Length = counters.EnqueuedCount,
                    Fetched = counters.FetchedCount,
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
                    Name = server.ServerId,
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
                    SingleOrDefault(x => x.JobId == id);

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

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var queueApi = GetQueueApi(queue);
            var fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);
            return FetchedJobs(fetchedJobIds.ToArray());
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobs(from, count, ProcessingState.StateName, (sqlJob, job, stateData) =>
                new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"]),
                });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobs(from, count, ScheduledState.StateName, (sqlJob, job, stateData) =>
                new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobs(from, count, SucceededState.StateName, (sqlJob, job, stateData) =>
                new SucceededJobDto
                {
                    Job = job,
                    Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? long.Parse(stateData["PerformanceDuration"]) + long.Parse(stateData["Latency"])
                        : default(long?),
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobs(from, count, FailedState.StateName, (sqlJob, job, stateData) =>
                new FailedJobDto
                {
                    Job = job,
                    Reason = sqlJob.ActualState.State.Reason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                });
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobs(from, count, DeletedState.StateName, (sqlJob, job, stateData) =>
                new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                });
        }

        public long ScheduledCount() => GetNumberOfJobsByStateName(ScheduledState.StateName);

        public long EnqueuedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetJobQueueCounters(queue);
            return counters.EnqueuedCount;
        }

        public long FetchedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetJobQueueCounters(queue);
            return counters.FetchedCount;
        }

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
                    where enqueuedJobIds.Contains(job.JobId)
                    orderby job.CreatedAt ascending
                    select job).
                    ToArray();

                return DeserializeJobs(
                    jobs,
                    (sqlJob, job, stateData) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = sqlJob.ActualState.State.Name,
                        EnqueuedAt = sqlJob.ActualState.State.Name == EnqueuedState.StateName
                            ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                            : null
                    });
            });
        }

        private JobList<FetchedJobDto> FetchedJobs(Guid[] fetchedJobIds)
        {
            return UseHangfireDbContext(context =>
            {
                var jobs = (
                    from job in context.Jobs.Include(x => x.ActualState.State)
                    where fetchedJobIds.Contains(job.JobId)
                    orderby job.CreatedAt ascending
                    select job).
                    ToArray();

                return new JobList<FetchedJobDto>(jobs.ToDictionary(x => x.JobId.ToString(), x => new FetchedJobDto
                {
                    State = x.ActualState.State.Name,
                    Job = DeserializeJob(x.InvocationData),
                }));
            });
        }

        private JobList<T> GetJobs<T>(
            int from,
            int count,
            string stateName,
            Func<HangfireJob, Job, Dictionary<string, string>, T> selector)
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

        private JobList<T> DeserializeJobs<T>(HangfireJob[] jobs, Func<HangfireJob, Job, Dictionary<string, string>, T> selector)
        {
            var result = new List<KeyValuePair<string, T>>(jobs.Length);

            foreach (var job in jobs)
            {
                var dto = default(T);

                if (!string.IsNullOrWhiteSpace(job.InvocationData))
                {
                    var deserializedData = JobHelper.FromJson<Dictionary<string, string>>(job.ActualState.State.Data);
                    var stateData = deserializedData != null
                        ? new Dictionary<string, string>(deserializedData, StringComparer.OrdinalIgnoreCase)
                        : null;

                    dto = selector(job, DeserializeJob(job.InvocationData), stateData);
                }

                result.Add(new KeyValuePair<string, T>(job.JobId.ToString(), dto));
            }

            return new JobList<T>(result);
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
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x.ToString("yyyy-MM-dd-HH")}");

            return GetTimelineStats(keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var dates = new List<DateTime>();
            for (var i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x.ToString("yyyy-MM-dd")}");

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

        private T UseHangfireDbContext<T>(Func<HangfireDbContext, T> func) => Storage.UseHangfireDbContext(func);

        private static Job DeserializeJob(string invocationData)
        {
            var data = JobHelper.FromJson<InvocationData>(invocationData);

            return data.Deserialize();
        }
    }
}