// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Migrations;
using System.Linq;
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

        public EntityFrameworkJobStorageConnection([NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            Storage = storage;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction() =>
            new EntityFrameworkJobStorageTransaction(Storage);

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout) =>
            new EntityFrameworkJobStorageDistributedLock(Storage, resource, timeout);

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            InvocationData invocationData = InvocationData.Serialize(job);

            Guid jobId = Guid.NewGuid();

            return Storage.UseContext(context =>
            {
                HangfireJob hangfireJob = context.Jobs.Add(new HangfireJob
                {
                    Id = jobId,
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
                        JobId = jobId,
                        Name = parameter.Key,
                        Value = parameter.Value,
                    });

                context.SaveChanges();

                return jobId.ToString();
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

            Guid jobId = Guid.Parse(id);

            Storage.UseContext(context =>
            {
                context.JobParameters.AddOrUpdate(new HangfireJobParameter
                {
                    JobId = jobId,
                    Name = name,
                    Value = value,
                });

                context.SaveChanges();
            });
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Guid jobId;
            if (!Guid.TryParse(id, out jobId))
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

            Guid id;
            if (!Guid.TryParse(jobId, out id))
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
                    State = job.ActualState.State.State,
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
                State = jobInfo.State.ToStateName(),
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

            Guid id;
            if (!Guid.TryParse(jobId, out id))
                return null;

            var stateInfo = Storage.UseContext(context => (
                from actualState in context.JobActualStates
                where actualState.JobId == id
                let state = actualState.State
                select new
                {
                    state.State,
                    state.Reason,
                    state.Data,
                }).
                FirstOrDefault());

            if (stateInfo == null) return null;

            return new StateData
            {
                Data = JobHelper.FromJson<Dictionary<string, string>>(stateInfo.Data),
                Name = stateInfo.State.ToStateName(),
                Reason = stateInfo.Reason,
            };
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));

            if (context == null)
                throw new ArgumentNullException(nameof(context));

            var data = JobHelper.ToJson(new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow,
            });

            Storage.UseContext(dbContext =>
            {
                dbContext.ServerHosts.AddOrUpdate(new HangfireServerHost
                {
                    Id = EntityFrameworkJobStorage.ServerHostId,
                });

                dbContext.Servers.AddOrUpdate(new HangfireServer
                {
                    Id = serverId,
                    Data = data,
                    Heartbeat = DateTime.UtcNow,
                    ServerHostId = EntityFrameworkJobStorage.ServerHostId,
                });

                dbContext.SaveChanges();
            });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));

            Storage.UseContext(context =>
            {
                context.Servers.RemoveRange(context.Servers.Where(x => x.Id == serverId));
                context.SaveChanges();

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

                string[] serverIds = (
                    from server in context.Servers
                    where server.Heartbeat <= outdate
                    select server.Id).
                    ToArray();

                foreach (var serverId in serverIds)
                    context.Entry(new HangfireServer
                    {
                        Id = serverId,
                    }).
                    State = EntityState.Deleted;

                context.SaveChanges();

                context.ServerHosts.RemoveRange(
                    from host in context.ServerHosts
                    where !host.Servers.Any()
                    select host);

                context.SaveChanges();

                return serverIds.Length;
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

            Storage.UseContext(context =>
            {
                var hashes = keyValuePairs.Select(x => new HangfireHash
                {
                    Key = key,
                    Field = x.Key,
                    Value = x.Value,
                }).
                ToArray();

                using (var transaction = context.Database.BeginTransaction())
                {
                    context.Hashes.AddOrUpdate(hashes);
                    context.SaveChanges();
                    transaction.Commit();
                }
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

        private static void Swap<T>(ref T left, ref T right)
        {
            T temp = left;
            left = right;
            right = temp;
        }
    }
}