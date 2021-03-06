﻿// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobStorageConnection : JobStorageConnection
    {
        private EntityFrameworkJobStorage Storage { get; }
        private EntityFrameworkDistributedLockManager DistributedLockManager { get; }

        public EntityFrameworkJobStorageConnection([NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            Storage = storage;
            DistributedLockManager = new EntityFrameworkDistributedLockManager(Storage);
        }

        public override IWriteOnlyTransaction CreateWriteTransaction() =>
            new EntityFrameworkJobStorageTransaction(Storage);

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return DistributedLockManager.AcquireDistributedLock(resource, timeout);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            InvocationData invocationData = InvocationData.Serialize(job);

            return Storage.UseContext(context =>
            {
                HangfireJob hangfireJob = context.Jobs.Add(new HangfireJob
                {
                    CreatedAt = createdAt,
                    ExpireAt = createdAt + expireIn,
                    ClrType = invocationData.Type,
                    Method = invocationData.Method,
                    ArgumentTypes = invocationData.ParameterTypes,
                    Arguments = invocationData.Arguments,
                });

                foreach (var parameter in parameters)
                    context.JobParameters.Add(new HangfireJobParameter
                    {
                        Name = parameter.Key,
                        Value = parameter.Value,
                        Job = hangfireJob
                    });

                context.SaveChanges();

                return hangfireJob.Id.ToString(CultureInfo.InvariantCulture);
            });
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null)
                throw new ArgumentNullException(nameof(queues));

            if (queues.Length == 0)
                throw new ArgumentException(ErrorStrings.QueuesCannotBeEmpty, nameof(queues));

            var providers = queues
                .Select(queue => Storage.QueueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length > 1)
                throw new InvalidOperationException(string.Format(ErrorStrings.Culture,
                    ErrorStrings.MultipleQueueProvidersNotSupported, string.Join(", ", queues)));

            var persistentQueue = providers.First().GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            long jobId = long.Parse(id, CultureInfo.InvariantCulture);

            Storage.UseContext(context =>
            {
                var parameter = new HangfireJobParameter
                {
                    JobId = jobId,
                    Name = name,
                    Value = value,
                };

                if (!context.JobParameters.Any(x => x.JobId == jobId && x.Name == name))
                    context.JobParameters.Add(parameter);
                else
                    context.Entry(parameter).State = EntityState.Modified;

                context.SaveChanges();
            });
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            long jobId;
            if (!long.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out jobId))
                return null;

            return Storage.UseContext(context => (
                from parameter in context.JobParameters
                where parameter.JobId == jobId && parameter.Name == name
                select parameter.Value).
                SingleOrDefault());
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            long id;
            if (!long.TryParse(jobId, NumberStyles.Integer, CultureInfo.InvariantCulture, out id))
                return null;

            var jobInfo = Storage.UseContext(context => (
                from job in context.Jobs
                where job.Id == id
                select new
                {
                    job.ClrType,
                    job.Method,
                    job.ArgumentTypes,
                    job.Arguments,
                    job.CreatedAt,
                    State = job.ActualState,
                }).
                FirstOrDefault());

            if (jobInfo == null) return null;

            var invocationData = new InvocationData(
                jobInfo.ClrType,
                jobInfo.Method,
                jobInfo.ArgumentTypes,
                jobInfo.Arguments);

            var jobData = new JobData
            {
                State = jobInfo.State,
                CreatedAt = jobInfo.CreatedAt,
            };

            try
            {
                jobData.Job = invocationData.Deserialize();
            }
            catch (JobLoadException exception)
            {
                jobData.LoadException = exception;
            }

            return jobData;
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            long id;
            if (!long.TryParse(jobId, NumberStyles.Integer, CultureInfo.InvariantCulture, out id))
                return null;

            var stateInfo = Storage.UseContext(context => (
                from job in context.Jobs
                where job.Id == id
                from state in job.States
                where
                    job.ActualState != null &&
                    job.ActualState == state.Name
                orderby state.CreatedAt descending
                select new
                {
                    state.Name,
                    state.Reason,
                    state.Data,
                }).
                FirstOrDefault());

            if (stateInfo == null)
                return null;

            return new StateData
            {
                Data = JobHelper.FromJson<Dictionary<string, string>>(stateInfo.Data),
                Name = stateInfo.Name,
                Reason = stateInfo.Reason,
            };
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));

            if (context == null)
                throw new ArgumentNullException(nameof(context));

            var queues = JobHelper.ToJson(context.Queues);
            var timestamp = DateTime.UtcNow;

            Storage.UseContext(dbContext =>
            {
                var serverHost = new HangfireServerHost
                {
                    Id = EntityFrameworkJobStorage.ServerHostId,
                };

                var serverHosts = dbContext.ServerHosts;

                if (!serverHosts.Any(x => x.Id == EntityFrameworkJobStorage.ServerHostId))
                    serverHosts.Add(serverHost);

                var server = new HangfireServer
                {
                    Id = serverId,
                    StartedAt = timestamp,
                    Heartbeat = timestamp,
                    WorkerCount = context.WorkerCount,
                    ServerHostId = EntityFrameworkJobStorage.ServerHostId,
                    Queues = queues,
                };

                var servers = dbContext.Servers;

                if (!servers.Any(x => x.Id == serverId))
                    servers.Add(server);
                else
                    dbContext.Entry(server).State = EntityState.Modified;

                dbContext.SaveChanges();
            });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));

            Storage.UseContext(context =>
            {
                RemoveServers(context, x => x.Id == serverId);

                context.ServerHosts.RemoveRange(
                    from host in context.ServerHosts
                    where !host.Servers.Any()
                    select host);

                context.SaveChanges();
            });
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));

            Storage.UseContext(context =>
            {
                if (context.Servers.Any(x => x.Id == serverId))
                {
                    var server = new HangfireServer
                    {
                        Id = serverId,
                        Heartbeat = DateTime.UtcNow,
                    };

                    context.Servers.Attach(server);

                    context.Entry(server).Property(x => x.Heartbeat).IsModified = true;

                    context.SaveChanges();
                }
            });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(timeOut), timeOut, ErrorStrings.NeedNonNegativeValue);

            return Storage.UseContext(context =>
            {
                DateTime outdate = DateTime.UtcNow - timeOut;
                int removed = RemoveServers(context, x => x.Heartbeat <= outdate);
                RemoveStaleServerHosts(context);

                return removed;
            });
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return Storage.UseContext(context => new HashSet<string>(
                from set in context.Sets
                where set.Key == key
                select set.Value));
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (toScore < fromScore)
                Swap(ref fromScore, ref toScore);

            return Storage.UseContext(context => (
                from set in context.Sets
                where set.Key == key && fromScore <= set.Score && set.Score <= toScore
                orderby set.Score
                select set.Value).
                FirstOrDefault());
        }

        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (endingAt < startingFrom)
                Swap(ref startingFrom, ref endingAt);

            int take = endingAt - startingFrom + 1;

            return Storage.UseContext(context => (
                from set in context.Sets
                where set.Key == key
                orderby set.CreatedAt
                select set.Value).
                Skip(() => startingFrom).
                Take(() => take).
                ToList());
        }

        public override long GetSetCount([NotNull] string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return Storage.UseContext(context => context.Sets.LongCount(x => x.Key == key));
        }

        public override TimeSpan GetSetTtl([NotNull] string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            DateTime? minExpiredAt = Storage.UseContext(context => (
                from set in context.Sets
                where set.Key == key
                select set.ExpireAt).
                Min());

            return minExpiredAt.HasValue ?
                minExpiredAt.Value - DateTime.UtcNow :
                TimeSpan.FromSeconds(-1);
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (keyValuePairs == null)
                throw new ArgumentNullException(nameof(keyValuePairs));

            var hashes = keyValuePairs.Select(x => new HangfireHash
            {
                Key = key,
                Field = x.Key,
                Value = x.Value,
            });

            Storage.UseContext(context =>
            {
                var fields = new HashSet<string>(
                    from hash in context.Hashes
                    where hash.Key == key
                    select hash.Field);

                foreach (var hash in hashes)
                    if (!fields.Contains(hash.Field))
                        context.Hashes.Add(hash);
                    else
                        context.Entry(hash).State = EntityState.Modified;

                context.SaveChanges();
            });
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            var result = Storage.UseContext(context => (
                from hash in context.Hashes
                where hash.Key == key
                select new
                {
                    hash.Field,
                    hash.Value,
                }).
                ToArray());

            if (result.Length == 0)
                return null;

            return result.ToDictionary(x => x.Field, x => x.Value);
        }

        public override long GetHashCount([NotNull] string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return Storage.UseContext(context => context.Hashes.LongCount(x => x.Key == key));
        }

        public override TimeSpan GetHashTtl([NotNull] string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            DateTime? minExpiredAt = Storage.UseContext(context => (
                from hash in context.Hashes
                where hash.Key == key
                select hash.ExpireAt).
                Min());

            return minExpiredAt.HasValue ? minExpiredAt.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override string GetValueFromHash([NotNull] string key, [NotNull] string name)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return Storage.UseContext(context => (
                from hash in context.Hashes
                where hash.Key == key && hash.Field == name
                select hash.Value).
                SingleOrDefault());
        }

        public override long GetCounter([NotNull] string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return Storage.UseContext(context => (
                from counter in context.Counters
                where counter.Key == key
                select (long?)counter.Value).
                Sum() ?? 0);
        }

        public override List<string> GetAllItemsFromList([NotNull] string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return Storage.UseContext(context => (
                from listItem in context.Lists
                where listItem.Key == key
                orderby listItem.Position descending
                select listItem.Value).
                ToList());
        }

        public override long GetListCount([NotNull] string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return Storage.UseContext(context => context.Lists.LongCount(x => x.Key == key));
        }

        public override TimeSpan GetListTtl([NotNull] string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            DateTime? minExpiredAt = Storage.UseContext(context => (
                from listItem in context.Lists
                where listItem.Key == key
                select listItem.ExpireAt).
                Min());

            return minExpiredAt.HasValue ?
                minExpiredAt.Value - DateTime.UtcNow :
                TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (endingAt < startingFrom)
                Swap(ref startingFrom, ref endingAt);

            int take = endingAt - startingFrom + 1;

            return Storage.UseContext(context => (
                from listItem in context.Lists
                where listItem.Key == key
                orderby listItem.Position descending
                select listItem.Value).
                Skip(() => startingFrom).
                Take(() => take).
                ToList());
        }

        private static void RemoveStaleServerHosts(HangfireDbContext context)
        {
            context.ServerHosts.RemoveRange(
                from host in context.ServerHosts
                where !host.Servers.Any()
                select host);

            context.SaveChanges();
        }

        private static int RemoveServers(HangfireDbContext context, Expression<Func<HangfireServer, bool>> predicate)
        {
            var servers = context.Servers.Where(predicate);

            context.JobQueues.RemoveRange(
                from server in servers
                from queueItem in server.ServerHost.QueueItems
                select queueItem);

            string[] serverIds = (
                from server in servers
                select server.Id).
                ToArray();

            foreach (var serverId in serverIds)
                context.Entry(new HangfireServer
                {
                    Id = serverId,
                }).
                State = EntityState.Deleted;

            context.SaveChanges();

            return serverIds.Length;
        }

        private static void Swap<T>(ref T left, ref T right)
        {
            T temp = left;
            left = right;
            right = temp;
        }
    }
}