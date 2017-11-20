// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Migrations;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobStorageTransaction : JobStorageTransaction
    {
        private Queue<Action<HangfireDbContext>> CommandQueue { get; } = new Queue<Action<HangfireDbContext>>();
        private Queue<Action> AfterCommitCommandQueue { get; } = new Queue<Action>();

        private EntityFrameworkJobStorage Storage { get; }

        public EntityFrameworkJobStorageTransaction([NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            Storage = storage;
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            EnqueueCommand(context =>
            {
                var job = new HangfireJob
                {
                    Id = Guid.Parse(jobId),
                    ExpireAt = DateTime.UtcNow + expireIn,
                    InvocationData = string.Empty,
                };
                context.Jobs.Attach(job);
                context.Entry(job).Property(x => x.ExpireAt).IsModified = true;
            });
        }

        public override void PersistJob(string jobId)
        {
            EnqueueCommand(context =>
            {
                var job = new HangfireJob
                {
                    Id = Guid.Parse(jobId),
                    InvocationData = string.Empty,
                };
                context.Jobs.Attach(job);
                context.Entry(job).Property(x => x.ExpireAt).IsModified = true;
            });
        }

        public override void SetJobState(string jobId, IState state)
        {
            EnqueueCommand(context =>
            {
                Guid id = Guid.Parse(jobId);
                Guid stateId = AddJobStateToContext(context, id, state);
                context.JobActualStates.AddOrUpdate(new HangfireJobActualState { JobId = id, StateId = stateId, });
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            EnqueueCommand(context => AddJobStateToContext(context, Guid.Parse(jobId), state));
        }

        private Guid AddJobStateToContext(HangfireDbContext context, Guid jobId, IState state)
        {
            Guid stateId = Guid.NewGuid();
            var jobState = new HangfireJobState
            {
                Id = stateId,
                JobId = jobId,
                State = JobStateExtensions.ToJobState(state.Name),
                Reason = state.Reason,
                Data = JobHelper.ToJson(state.SerializeData()),
                CreatedAt = DateTime.UtcNow,
            };
            context.JobStates.Add(jobState);
            return stateId;
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var provider = Storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();
            EnqueueCommand(context => persistentQueue.Enqueue(queue, jobId));

            if (persistentQueue.GetType() == typeof(EntityFrameworkJobQueue))
                AfterCommitCommandQueue.Enqueue(() => EntityFrameworkJobQueue.NewItemInQueueEvent.Set());
        }

        public override void IncrementCounter(string key)
        {
            EnqueueCommand(context => AddCounterToContext(context, key, 1, null));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            EnqueueCommand(context => AddCounterToContext(context, key, 1, expireIn));
        }

        public override void DecrementCounter(string key)
        {
            EnqueueCommand(context => AddCounterToContext(context, key, -1, null));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            EnqueueCommand(context => AddCounterToContext(context, key, -1, expireIn));
        }

        private void AddCounterToContext(HangfireDbContext context, string key, long value, TimeSpan? expireIn)
        {
            var counter = new HangfireCounter
            {
                Id = Guid.NewGuid(),
                Key = key,
                Value = value,
            };

            if (expireIn.HasValue)
                counter.ExpireAt = DateTime.UtcNow + expireIn;

            context.Counters.Add(counter);
        }

        public override void AddToSet(string key, string value) => AddToSet(key, value, 0);

        public override void AddToSet(string key, string value, double score)
        {
            EnqueueCommand(context =>
            {
                var set = new HangfireSet
                {
                    CreatedAt = DateTime.UtcNow,
                    Key = key,
                    Score = score,
                    Value = value
                };

                context.Sets.AddOrUpdate(set);
            });
        }

        public override void RemoveFromSet(string key, string value)
        {
            EnqueueCommand(context =>
            {
                var entries = context.ChangeTracker.Entries<HangfireSet>().
                    Where(x => x.Entity.Key == key && x.Entity.Value == value);

                foreach (var entry in entries)
                    entry.State = EntityState.Detached;

                if (context.Sets.Any(x => x.Key == key && x.Value == value))
                    context.Entry(new HangfireSet { Key = key, Value = value }).State = EntityState.Deleted;
            });
        }

        public override void AddRangeToSet([NotNull] string key, [NotNull] IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            EnqueueCommand(context =>
            {
                var exisitingFields = new HashSet<string>(
                    from set in context.Sets
                    where set.Key == key
                    select set.Value);

                foreach (var item in items)
                {
                    var set = new HangfireSet { Key = key, Value = item, };

                    if (exisitingFields.Contains(item))
                    {
                        context.Sets.Attach(set);
                        context.Entry(set).Property(x => x.Value).IsModified = true;
                    }
                    else
                    {
                        set.CreatedAt = DateTime.UtcNow;
                        context.Sets.Add(set);
                    }
                }
            });
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            EnqueueCommand(context =>
            {
                var ids = (
                from item in context.Sets
                where item.Key == key
                select new { item.Key, item.Value }).
                ToArray();

                DateTime expireAt = DateTime.UtcNow.Add(expireIn);

                foreach (var id in ids)
                {
                    var item = new HangfireSet { Key = id.Key, Value = id.Value, ExpireAt = expireAt, };
                    context.Sets.Attach(item);
                    context.Entry(item).Property(x => x.ExpireAt).IsModified = true;
                }
            });
        }

        public override void PersistSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            EnqueueCommand(context =>
            {
                var ids = (
                from item in context.Sets
                where item.Key == key
                select new { item.Key, item.Value }).
                ToArray();

                foreach (var id in ids)
                {
                    var item = new HangfireSet { Key = id.Key, Value = id.Value, };
                    context.Sets.Attach(item);
                    context.Entry(item).Property(x => x.ExpireAt).IsModified = true;
                }
            });
        }

        public override void RemoveSet([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            EnqueueCommand(context =>
            {
                string[] values = (
                from set in context.Sets
                where set.Key == key
                select set.Value).
                ToArray();

                foreach (var value in values)
                    context.Entry(new HangfireSet { Key = key, Value = value, }).State = EntityState.Deleted;
            });
        }

        public override void InsertToList(string key, string value)
        {
            EnqueueCommand(context =>
            {
                context.Lists.Add(new HangfireListItem
                {
                    Key = key,
                    Position = (context.Lists.Where(x => x.Key == key).Max(x => (int?)x.Position) ?? -1) + 1,
                    Value = value,
                });
            });
        }

        public override void RemoveFromList(string key, string value)
        {
            EnqueueCommand(context =>
            {
                var list = (
                    from item in context.Lists
                    where item.Key == key
                    orderby item.Position
                    select item).
                    ToArray();

                var newList = list.Where(x => x.Value != value).ToArray();

                for (int i = newList.Length; i < list.Length; i++)
                    context.Lists.Remove(list[i]);

                CopyNonKeyValues(newList, list);
            });
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            EnqueueCommand(context =>
            {
                var list = (
                    from item in context.Lists
                    where item.Key == key
                    orderby item.Position
                    select item).
                    ToArray();

                var newList = list.
                    Where((item, index) => keepStartingFrom <= index && index <= keepEndingAt).
                    ToArray();

                for (int i = newList.Length; i < list.Length; i++)
                    context.Lists.Remove(list[i]);

                CopyNonKeyValues(newList, list);
            });
        }

        private static void CopyNonKeyValues(HangfireListItem[] source, HangfireListItem[] destination)
        {
            for (int i = 0; i < source.Length; i++)
            {
                var oldItem = destination[i];
                var newItem = source[i];

                if (ReferenceEquals(oldItem, newItem))
                    continue;

                if (oldItem.ExpireAt != newItem.ExpireAt)
                    oldItem.ExpireAt = newItem.ExpireAt;
                if (oldItem.Value != newItem.Value)
                    oldItem.Value = newItem.Value;
            }
        }

        public override void ExpireList([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            EnqueueCommand(context =>
            {
                var ids = (
                from item in context.Lists
                where item.Key == key
                select new { item.Key, item.Position }).
                ToArray();

                DateTime expireAt = DateTime.UtcNow.Add(expireIn);

                foreach (var id in ids)
                {
                    var item = new HangfireListItem { Key = id.Key, Position = id.Position, ExpireAt = expireAt };
                    context.Lists.Attach(item);
                    context.Entry(item).Property(x => x.ExpireAt).IsModified = true;
                }
            });
        }

        public override void PersistList([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            EnqueueCommand(context =>
            {
                var ids = (
                    from item in context.Lists
                    where item.Key == key
                    select new { item.Key, item.Position }).
                    ToArray();

                foreach (var id in ids)
                {
                    var item = new HangfireListItem { Key = id.Key, Position = id.Position, };
                    context.Lists.Attach(item);
                    context.Entry(item).Property(x => x.ExpireAt).IsModified = true;
                }
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            EnqueueCommand(context =>
            {
                var exisitingFields = new HashSet<string>(
                    from hash in context.Hashes
                    where hash.Key == key
                    select hash.Field);

                foreach (var item in keyValuePairs)
                {
                    var hash = new HangfireHash { Key = key, Field = item.Key, Value = item.Value };

                    if (exisitingFields.Contains(item.Key))
                    {
                        context.Hashes.Attach(hash);
                        context.Entry(hash).Property(x => x.Value).IsModified = true;
                    }
                    else
                    {
                        context.Hashes.Add(hash);
                    }
                }
            });
        }

        public override void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            EnqueueCommand(context =>
            {
                string[] fields = (
                from hash in context.Hashes
                where hash.Key == key
                select hash.Field).
                ToArray();

                foreach (var field in fields)
                    context.Entry(new HangfireHash { Key = key, Field = field, }).State = EntityState.Deleted;
            });
        }

        public override void ExpireHash([NotNull] string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            EnqueueCommand(context =>
            {
                string[] fields = (
                from hash in context.Hashes
                where hash.Key == key
                select hash.Field).
                ToArray();

                DateTime expireAt = DateTime.UtcNow.Add(expireIn);

                foreach (var field in fields)
                {
                    var hash = new HangfireHash { Key = key, Field = field, ExpireAt = expireAt };
                    context.Hashes.Attach(hash);
                    context.Entry(hash).Property(x => x.ExpireAt).IsModified = true;
                }
            });
        }

        public override void PersistHash([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            EnqueueCommand(context =>
            {
                string[] fields = (
                from hash in context.Hashes
                where hash.Key == key
                select hash.Field).
                ToArray();

                foreach (var field in fields)
                {
                    var hash = new HangfireHash { Key = key, Field = field, ExpireAt = null };
                    context.Hashes.Attach(hash);
                    context.Entry(hash).Property(x => x.ExpireAt).IsModified = true;
                }
            });
        }

        public override void Commit()
        {
            using (var context = Storage.CreateContext())
            using (var transaction = context.Database.BeginTransaction())
            {
                while (CommandQueue.Count > 0)
                {
                    var action = CommandQueue.Dequeue();
                    action.Invoke(context);
                    context.SaveChanges();
                }
                transaction.Commit();
            }

            while (AfterCommitCommandQueue.Count > 0)
            {
                var action = AfterCommitCommandQueue.Dequeue();
                action.Invoke();
            }
        }

        private void EnqueueCommand(Action<HangfireDbContext> command) => CommandQueue.Enqueue(command);
    }
}