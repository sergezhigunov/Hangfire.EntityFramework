// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Globalization;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkJobStorageTransaction : JobStorageTransaction
    {
        private Queue<Action<HangfireDbContext>> CommandQueue { get; } =
            new Queue<Action<HangfireDbContext>>();

        private Queue<Action> AfterCommitCommandQueue { get; } =
            new Queue<Action>();

        private EntityFrameworkJobStorage Storage { get; }

        private bool Disposed { get; set; }

        public EntityFrameworkJobStorageTransaction([NotNull] EntityFrameworkJobStorage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            Storage = storage;
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            ValidateJobId(jobId);
            ThrowIfDisposed();

            long id = long.Parse(jobId, CultureInfo.InvariantCulture);

            EnqueueCommand(context =>
                SetJobExpiration(context, id, DateTime.UtcNow + expireIn));
        }

        public override void PersistJob(string jobId)
        {
            ValidateJobId(jobId);
            ThrowIfDisposed();

            long id = long.Parse(jobId, CultureInfo.InvariantCulture);

            EnqueueCommand(context =>
                SetJobExpiration(context, id, null));
        }

        public override void SetJobState(string jobId, IState state)
        {
            ValidateJobId(jobId);

            if (state == null)
                throw new ArgumentNullException(nameof(state));

            ThrowIfDisposed();

            long id = long.Parse(jobId, CultureInfo.InvariantCulture);

            EnqueueCommand(context => AddJobState(context, id, state, true));
        }

        public override void AddJobState(string jobId, IState state)
        {
            ValidateJobId(jobId);

            if (state == null)
                throw new ArgumentNullException(nameof(state));

            ThrowIfDisposed();

            long id = long.Parse(jobId, CultureInfo.InvariantCulture);

            EnqueueCommand(context => AddJobState(context, id, state, false));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            ValidateQueue(queue);
            ValidateJobId(jobId);
            ThrowIfDisposed();

            var provider = Storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();
            EnqueueCommand(context => persistentQueue.Enqueue(queue, jobId));

            if (persistentQueue.GetType() == typeof(EntityFrameworkJobQueue))
                AfterCommitCommandQueue.Enqueue(() => EntityFrameworkJobQueue.NewItemInQueueEvent.Set());
        }

        public override void IncrementCounter(string key)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context => AddCounterToContext(context, key, 1, null));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
                AddCounterToContext(context, key, 1, DateTime.UtcNow + expireIn));
        }

        public override void DecrementCounter(string key)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context => AddCounterToContext(context, key, -1, null));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
                AddCounterToContext(context, key, -1, DateTime.UtcNow + expireIn));
        }

        public override void AddToSet(string key, string value) => AddToSet(key, value, 0);

        public override void AddToSet(string key, string value, double score)
        {
            ValidateKey(key);

            ThrowIfDisposed();

            EnqueueCommand(context =>
            {
                var entry = context.ChangeTracker.
                    Entries<HangfireSet>().
                    FirstOrDefault(x => x.Entity.Key == key && x.Entity.Value == value);

                if (entry != null)
                {
                    var entity = entry.Entity;
                    entity.Score = score;
                    entity.CreatedAt = DateTime.UtcNow;
                    entry.State = EntityState.Modified;
                }
                else
                {
                    var set = new HangfireSet
                    {
                        CreatedAt = DateTime.UtcNow,
                        Key = key,
                        Score = score,
                        Value = value,
                    };

                    if (!context.Sets.Any(x => x.Key == key && x.Value == value))
                        context.Sets.Add(set);
                    else
                        context.Entry(set).State = EntityState.Modified;
                }
            });
        }

        public override void RemoveFromSet(string key, string value)
        {
            ValidateKey(key);

            ThrowIfDisposed();

            EnqueueCommand(context =>
            {
                var entries = context.ChangeTracker.Entries<HangfireSet>().
                    Where(x => x.Entity.Key == key && x.Entity.Value == value);

                foreach (var entry in entries)
                    entry.State = EntityState.Detached;

                if (context.Sets.Any(x => x.Key == key && x.Value == value))
                    context.Entry(new HangfireSet
                    {
                        Key = key,
                        Value = value
                    }).State = EntityState.Deleted;
            });
        }

        public override void AddRangeToSet([NotNull] string key, [NotNull] IList<string> items)
        {
            ValidateKey(key);

            if (items == null)
                throw new ArgumentNullException(nameof(items));

            ThrowIfDisposed();

            EnqueueCommand(context =>
            {
                var exisitingFields = new HashSet<string>(
                    from set in context.Sets
                    where set.Key == key
                    select set.Value);

                foreach (var item in items)
                {
                    var set = new HangfireSet
                    {
                        Key = key,
                        Value = item,
                    };

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
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
                SetSetExpiration(context, key, DateTime.UtcNow + expireIn));
        }

        public override void PersistSet([NotNull] string key)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
                SetSetExpiration(context, key, null));
        }

        public override void RemoveSet([NotNull] string key)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
            {
                string[] values = (
                    from set in context.Sets
                    where set.Key == key
                    select set.Value).
                    ToArray();

                foreach (var value in values)
                    context.Entry(new HangfireSet
                    {
                        Key = key,
                        Value = value,
                    }).State = EntityState.Deleted;
            });
        }

        public override void InsertToList(string key, string value)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
            {
                context.Lists.Add(new HangfireList
                {
                    Key = key,
                    Position = (
                        context.Lists.
                        Where(x => x.Key == key).
                        Max(x => (int?)x.Position) ?? -1) + 1,
                    Value = value,
                });
            });
        }

        public override void RemoveFromList(string key, string value)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
            {
                var list = (
                    from item in context.Lists
                    where item.Key == key
                    orderby item.Position
                    select item).
                    ToArray();

                var newList = list.
                    Where(x => x.Value != value).
                    ToArray();

                for (int i = newList.Length; i < list.Length; i++)
                    context.Lists.Remove(list[i]);

                CopyNonKeyValues(newList, list);
            });
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            ValidateKey(key);
            ThrowIfDisposed();

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

        public override void ExpireList([NotNull] string key, TimeSpan expireIn)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
                SetListexpiration(context, key, DateTime.UtcNow + expireIn));
        }

        public override void PersistList([NotNull] string key)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
                SetListexpiration(context, key, null));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            ValidateKey(key);

            if (keyValuePairs == null)
                throw new ArgumentNullException(nameof(keyValuePairs));

            ThrowIfDisposed();

            EnqueueCommand(context =>
            {
                var exisitingFields = new HashSet<string>(
                    from hash in context.Hashes
                    where hash.Key == key
                    select hash.Field);

                foreach (var item in keyValuePairs)
                {
                    var hash = new HangfireHash
                    {
                        Key = key,
                        Field = item.Key,
                        Value = item.Value,
                    };

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
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
            {
                string[] fields = (
                    from hash in context.Hashes
                    where hash.Key == key
                    select hash.Field).
                    ToArray();

                foreach (var field in fields)
                    context.Entry(new HangfireHash
                    {
                        Key = key,
                        Field = field,
                    }).State = EntityState.Deleted;
            });
        }

        public override void ExpireHash([NotNull] string key, TimeSpan expireIn)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
                SetHashExpiration(context, key, DateTime.UtcNow + expireIn));
        }

        public override void PersistHash([NotNull] string key)
        {
            ValidateKey(key);
            ThrowIfDisposed();

            EnqueueCommand(context =>
                SetHashExpiration(context, key, null));
        }

        public override void Commit()
        {
            ThrowIfDisposed();

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

        public sealed override void Dispose()
        {
            base.Dispose();

            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
            }

            Disposed = true;
        }

        protected void ThrowIfDisposed()
        {
            if (Disposed)
                throw new ObjectDisposedException(GetType().FullName);
        }

        private static void AddCounterToContext(HangfireDbContext context, string key, long value, DateTime? expireAt)
        {
            var counter = new HangfireCounter
            {
                Key = key,
                Value = value,
                ExpireAt = expireAt,
            };

            context.Counters.Add(counter);
        }

        private static void AddJobState(HangfireDbContext context, long id, IState state, bool setActual)
        {
            var addedState = context.JobStates.Add(new HangfireJobState
            {
                JobId = id,
                State = state.Name,
                Reason = state.Reason,
                Data = JobHelper.ToJson(state.SerializeData()),
                CreatedAt = DateTime.UtcNow,
            });

            if (setActual)
            {
                var entry = context.ChangeTracker.
                    Entries<HangfireJob>().
                    FirstOrDefault(x => x.Entity.Id == id);

                if (entry != null)
                    entry.Entity.ActualState = addedState.State;
                else
                {
                    entry = context.Entry(context.Jobs.Attach(new HangfireJob
                    {
                        Id = id,
                        ActualState = addedState.State,
                    }));
                }

                entry.Property(x => x.ActualState).IsModified = true;
            }
        }

        private static void CopyNonKeyValues(HangfireList[] source, HangfireList[] destination)
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

        private static void SetJobExpiration(HangfireDbContext context, long id, DateTime? expireAt)
        {
            var entry = context.ChangeTracker.
                Entries<HangfireJob>().
                FirstOrDefault(x => x.Entity.Id == id);

            if (entry != null)
                entry.Entity.ExpireAt = expireAt;
            else
            {
                entry = context.Entry(context.Jobs.Attach(new HangfireJob
                {
                    Id = id,
                    ExpireAt = expireAt,
                }));
            }

            entry.Property(x => x.ExpireAt).IsModified = true;
        }

        private static void SetHashExpiration(HangfireDbContext context, string key, DateTime? expireAt)
        {
            string[] fields = (
                from hash in context.Hashes
                where hash.Key == key
                select hash.Field).
                ToArray();

            foreach (var field in fields)
            {
                var hash = new HangfireHash
                {
                    Key = key,
                    Field = field,
                    ExpireAt = expireAt,
                };

                context.Hashes.Attach(hash);

                context.Entry(hash).Property(x => x.ExpireAt).IsModified = true;
            }
        }

        private static void SetListexpiration(HangfireDbContext context, string key, DateTime? expireAt)
        {
            var ids = (
                from item in context.Lists
                where item.Key == key
                select new
                {
                    item.Key,
                    item.Position,
                }).
                ToArray();

            foreach (var id in ids)
            {
                var item = new HangfireList
                {
                    Key = id.Key,
                    Position = id.Position,
                    ExpireAt = expireAt
                };

                context.Lists.Attach(item);

                context.Entry(item).Property(x => x.ExpireAt).IsModified = true;
            }
        }

        private static void SetSetExpiration(HangfireDbContext context, string key, DateTime? expireAt)
        {
            var ids = (
                from item in context.Sets
                where item.Key == key
                select new
                {
                    item.Key,
                    item.Value,
                }).
                ToArray();

            foreach (var id in ids)
            {
                var item = new HangfireSet
                {
                    Key = id.Key,
                    Value = id.Value,
                    ExpireAt = expireAt,
                };

                context.Sets.Attach(item);

                context.Entry(item).Property(x => x.ExpireAt).IsModified = true;
            }
        }

        private static void ValidateQueue(string queue)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));

            if (queue == string.Empty)
                throw new ArgumentException(ErrorStrings.StringCannotBeEmpty, nameof(queue));
        }

        private static void ValidateKey(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (key == string.Empty)
                throw new ArgumentException(ErrorStrings.StringCannotBeEmpty, nameof(key));
        }

        private static void ValidateJobId(string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            if (jobId == string.Empty)
                throw new ArgumentException(ErrorStrings.StringCannotBeEmpty, nameof(jobId));
        }
    }
}