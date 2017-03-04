// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using Hangfire.Common;
using Hangfire.EntityFramework.Utils;
using Hangfire.States;
using Moq;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class EntityFrameworkJobStorageTransactionTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobStorageTransaction(null));
        }

        [Fact, CleanDatabase]
        public void ExpireJob_SetsJobExpirationData()
        {
            Guid jobId = InsertTestJob();
            Guid anotherJobId = InsertTestJob();

            UseTransaction(transaction => transaction.ExpireJob(jobId.ToString(), TimeSpan.FromDays(1)));

            var job = GetTestJob(jobId);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) < job.ExpireAt && job.ExpireAt <= DateTime.UtcNow.AddDays(1));

            var anotherJob = GetTestJob(anotherJobId);
            Assert.Null(anotherJob.ExpireAt);
        }

        [Fact, CleanDatabase]
        public void PersistJob_ClearsTheJobExpirationData()
        {
            Guid jobId = InsertTestJob(DateTime.UtcNow);
            Guid anotherJobId = InsertTestJob(DateTime.UtcNow);

            UseTransaction(transaction => transaction.PersistJob(jobId.ToString()));

            var job = GetTestJob(jobId);
            Assert.Null(job.ExpireAt);

            var anotherJob = GetTestJob(anotherJobId);
            Assert.NotNull(anotherJob.ExpireAt);
        }

        [Fact, CleanDatabase]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            Guid jobId = InsertTestJob();
            Guid anotherJobId = InsertTestJob();

            var state = new Mock<IState>();
            state.Setup(x => x.Name).Returns("State");
            state.Setup(x => x.Reason).Returns("Reason");
            state.Setup(x => x.SerializeData()).
                Returns(new Dictionary<string, string> { { "Name", "Value" } });

            DateTime beginTimestamp = DateTime.UtcNow.AddSeconds(-1);
            UseTransaction(transaction => transaction.SetJobState(jobId.ToString(), state.Object));
            DateTime endTimestamp = DateTime.UtcNow.AddSeconds(1);

            var job = GetTestJob(jobId);
            Assert.Equal("State", job.ActualState.State.Name);

            var anotherJob = GetTestJob(anotherJobId);
            Assert.Null(anotherJob.ActualState);

            var jobState = job.ActualState.State;
            Assert.Equal("State", jobState.Name);
            Assert.Equal("Reason", jobState.Reason);
            Assert.True(beginTimestamp <= jobState.CreatedAt && jobState.CreatedAt <= endTimestamp);
            var data = JobHelper.FromJson<Dictionary<string, string>>(jobState.Data);
            Assert.Equal(1, data.Count);
            Assert.Equal("Value", data["Name"]);
        }

        [Fact, CleanDatabase]
        public void AddJobState_JustAddsANewRecordInATable()
        {
            Guid jobId = InsertTestJob();

            var state = new Mock<IState>();
            state.Setup(x => x.Name).Returns("State");
            state.Setup(x => x.Reason).Returns("Reason");
            state.Setup(x => x.SerializeData()).
                Returns(new Dictionary<string, string> { { "Name", "Value" } });

            DateTime beginTimestamp = DateTime.UtcNow.AddSeconds(-1);
            UseTransaction(transaction => transaction.AddJobState(jobId.ToString(), state.Object));
            DateTime endTimestamp = DateTime.UtcNow.AddSeconds(1);

            var job = GetTestJob(jobId);
            Assert.Null(job.ActualState);

            var jobState = job.States.Single();
            Assert.Equal("State", jobState.Name);
            Assert.Equal("Reason", jobState.Reason);
            Assert.True(beginTimestamp <= jobState.CreatedAt && jobState.CreatedAt <= endTimestamp);
            var data = JobHelper.FromJson<Dictionary<string, string>>(jobState.Data);
            Assert.Equal(1, data.Count);
            Assert.Equal("Value", data["Name"]);
        }

        [Fact, CleanDatabase]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            var id = InsertTestJob();

            UseTransaction(transaction => transaction.AddToQueue("default", id.ToString()));

            var result = UseContext(context => context.JobQueues.Single());

            Assert.Equal(id, result.JobId);
            Assert.Equal("default", result.Queue);
            Assert.Null(result.FetchedAt);
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.IncrementCounter(key));

            var record = UseContext(context => context.Counters.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal(1, record.Value);
            Assert.Null(record.ExpireAt);
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.IncrementCounter(key, TimeSpan.FromDays(1)));

            var record = UseContext(context => context.Counters.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal(1, record.Value);
            Assert.NotNull(record.ExpireAt);

            var expireAt = (DateTime)record.ExpireAt;

            Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
            Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.IncrementCounter(key);
                transaction.IncrementCounter(key);
            });

            var count = UseContext(context => context.Counters.Count());
            var sum = UseContext(context => context.Counters.Sum(x => x.Value));

            Assert.Equal(2, count);
            Assert.Equal(2, sum);
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.DecrementCounter(key));

            var record = UseContext(context => context.Counters.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal(-1, record.Value);
            Assert.Null(record.ExpireAt);
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.DecrementCounter(key, TimeSpan.FromDays(1)));

            var record = UseContext(context => context.Counters.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal(-1, record.Value);
            Assert.NotNull(record.ExpireAt);

            var expireAt = (DateTime)record.ExpireAt;

            Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
            Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.DecrementCounter(key);
                transaction.DecrementCounter(key);
            });

            var count = UseContext(context => context.Counters.Count());
            var sum = UseContext(context => context.Counters.Sum(x => x.Value));

            Assert.Equal(2, count);
            Assert.Equal(-2, sum);
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.AddToSet(key, "my-value"));

            var record = UseContext(context => context.Sets.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal("my-value", record.Value);
            Assert.Equal(0.0, record.Score, 2);
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.AddToSet(key, "my-value");
                transaction.AddToSet(key, "another-value");
            });

            var recordCount = UseContext(context => context.Sets.Count());

            Assert.Equal(2, recordCount);
        }

        [Fact, CleanDatabase]
        public void AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.AddToSet(key, "my-value");
                transaction.AddToSet(key, "my-value");
            });

            var recordCount = UseContext(context => context.Sets.Count());

            Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.AddToSet(key, "my-value", 3.2));

            var record = UseContext(context => context.Sets.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal("my-value", record.Value);
            Assert.Equal(3.2, record.Score, 3);
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.AddToSet(key, "my-value");
                transaction.AddToSet(key, "my-value", 3.2);
            });

            var record = UseContext(context => context.Sets.Single());

            Assert.Equal(3.2, record.Score, 3);
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.AddToSet(key, "my-value");
                transaction.RemoveFromSet(key, "my-value");
            });

            var recordCount = UseContext(context => context.Sets.Count());

            Assert.Equal(0, recordCount);
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.AddToSet(key, "my-value");
                transaction.RemoveFromSet(key, "another-value");
            });

            var recordCount = UseContext(context => context.Sets.Count());

            Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.AddToSet(key, "my-value");
                transaction.RemoveFromSet("another-key", "my-value");
            });

            var recordCount = UseContext(context => context.Sets.Count());

            Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsARecord_WithGivenValues()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.InsertToList(key, "my-value"));

            var record = UseContext(context => context.Lists.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal("my-value", record.Value);
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.InsertToList(key, "my-value");
                transaction.InsertToList(key, "my-value");
            });

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(2, recordCount);
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.InsertToList(key, "my-value");
                transaction.InsertToList(key, "my-value");
                transaction.RemoveFromList(key, "my-value");
            });

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(0, recordCount);
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.InsertToList(key, "my-value");
                transaction.RemoveFromList(key, "different-value");
            });

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.InsertToList(key, "my-value");
                transaction.RemoveFromList("different-key", "my-value");
            });

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public void TrimList_TrimsAList_ToASpecifiedRange()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction =>
            {
                transaction.InsertToList(key, "0");
                transaction.InsertToList(key, "1");
                transaction.InsertToList(key, "2");
                transaction.InsertToList(key, "3");
                transaction.TrimList(key, 1, 2);
            });

            var records = UseContext(context => context.Lists.ToArray());

            Assert.Equal(2, records.Length);
            Assert.Equal("1", records[0].Value);
            Assert.Equal("2", records[1].Value);
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecordsToEnd_IfKeepAndingAt_GreaterThanMaxElementIndex()
        {
            string key = Guid.NewGuid().ToString();
            UseContextWithSavingChanges(context => context.Lists.AddRange(new []
            {
                new HangfireListItem { Key = key, Position = 0, Value = "0", },
                new HangfireListItem { Key = key, Position = 1, Value = "1", },
                new HangfireListItem { Key = key, Position = 2, Value = "2", },
            }));

            UseTransaction(transaction => transaction.TrimList(key, 1, 100));

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(2, recordCount);
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
        {
            string key = Guid.NewGuid().ToString();
            UseContextWithSavingChanges(context => context.Lists.
                Add(new HangfireListItem { Key = key, Position = 0, Value = "0", }));

            UseTransaction(transaction => transaction.TrimList(key, 1, 100));

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(0, recordCount);
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
        {
            string key = Guid.NewGuid().ToString();
            UseContextWithSavingChanges(context => context.Lists.
                Add(new HangfireListItem { Key = key, Position = 0, Value = "0", }));

            UseTransaction(transaction => transaction.TrimList(key, 1, 0));

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(0, recordCount);
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecords_OnlyOfAGivenKey()
        {
            string key = Guid.NewGuid().ToString();
            string anotherKey = Guid.NewGuid().ToString();
            UseContextWithSavingChanges(context => context.Lists.
                Add(new HangfireListItem { Key = key, Position = 0, Value = "0", }));

            UseTransaction(transaction => transaction.TrimList(anotherKey, 1, 0));

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("key",
                () => transaction.SetRangeInHash(null, new Dictionary<string, string>())));
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("keyValuePairs",
                () => transaction.SetRangeInHash("some-hash", null)));
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            string key = Guid.NewGuid().ToString();
            var keyValuePairs = new Dictionary<string, string>
            {
                ["Key1"] = "Value1",
                ["Key2"] = "Value2",
            };

            UseTransaction(transaction => transaction.SetRangeInHash(key, keyValuePairs));

            var result = UseContext(context => context.Hashes.
                Where(x => x.Key == key).
                ToDictionary(x => x.Field, x => x.Value));

            Assert.Equal("Value1", result["Key1"]);
            Assert.Equal("Value2", result["Key2"]);
        }

        [Fact, CleanDatabase]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("key",
                () => transaction.RemoveHash(null)));
        }

        [Fact, CleanDatabase]
        public void RemoveHash_RemovesAllHashRecords()
        {
            string key = Guid.NewGuid().ToString();
            var keyValuePairs = new Dictionary<string, string>
            {
                ["Key1"] = "Value1",
                ["Key2"] = "Value2",
            };
            UseTransaction(transaction => transaction.SetRangeInHash(key, keyValuePairs));

            UseTransaction(transaction => transaction.RemoveHash(key));

            var count = UseContext(context => context.Hashes.Count());
            Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>Assert.Throws<ArgumentNullException>("key",
                () => transaction.AddRangeToSet(null, new List<string>())));
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_ThrowsAnException_WhenItemsValueIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("items",
                () => transaction.AddRangeToSet("my-set", null)));
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_AddsAllItems_ToAGivenSet()
        {
            string setId = Guid.NewGuid().ToString();
            var items = new List<string> { "1", "2", "3" };

            UseTransaction(transaction => transaction.AddRangeToSet(setId, items));

            var records = UseContext(context => (
                from set in context.Sets
                where set.Key == setId
                select set.Value).
                ToArray());

            Assert.Equal(items, records);
        }

        [Fact, CleanDatabase]
        public void RemoveSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("key",
                () => transaction.RemoveSet( null)));
        }

        [Fact, CleanDatabase]
        public void RemoveSet_RemovesASet_WithAGivenKey()
        {
            var sets = new[]
            {
                new HangfireSet { Key = "set-1", Value = "1", CreatedAt = DateTime.UtcNow, },
                new HangfireSet { Key = "set-2", Value = "1", CreatedAt = DateTime.UtcNow, },
            };
            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            UseTransaction(transaction => transaction.RemoveSet("set-1"));

            var record = UseContext(context => context.Sets.Single());
            Assert.Equal("set-2", record.Key);
        }

        [Fact, CleanDatabase]
        public void ExpireHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("key",
                () => transaction.ExpireHash(null, TimeSpan.FromMinutes(5))));
        }

        [Fact, CleanDatabase]
        public void ExpireHash_SetsExpirationTimeOnAHash_WithGivenKey()
        {
            var hashes = new[]
            {
                new HangfireHash { Key = "hash-1", Field = "field", },
                new HangfireHash { Key = "hash-2", Field = "field", },
            };
            UseContextWithSavingChanges(context => context.Hashes.AddRange(hashes));

            UseTransaction(transaction => transaction.ExpireHash("hash-1", new TimeSpan(1, 0, 0)));

            var records = UseContext(context => context.Hashes.ToDictionary(x => x.Key, x => x.ExpireAt));

            Assert.True(DateTime.UtcNow.AddMinutes(59) < records["hash-1"]);
            Assert.True(records["hash-1"] < DateTime.UtcNow.AddMinutes(61));
            Assert.Null(records["hash-2"]);
        }

        [Fact, CleanDatabase]
        public void ExpireSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("key",
                () => transaction.ExpireSet(null, new TimeSpan(0, 0, 45))));
        }

        [Fact, CleanDatabase]
        public void ExpireSet_SetsExpirationTime_OnASet_WithGivenKey()
        {
            var sets = new[]
            {
                new HangfireSet { Key = "set-1", Value = "1", CreatedAt = DateTime.UtcNow, },
                new HangfireSet { Key = "set-2", Value = "1", CreatedAt = DateTime.UtcNow, },
            };
            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            UseTransaction(transaction => transaction.ExpireSet("set-1", new TimeSpan(1, 0, 0)));

            var records = UseContext(context => context.Sets.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.True(DateTime.UtcNow.AddMinutes(59) < records["set-1"]);
            Assert.True(records["set-1"] < DateTime.UtcNow.AddMinutes(61));
            Assert.Null(records["set-2"]);
        }

        [Fact, CleanDatabase]
        public void ExpireList_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("key",
                () => transaction.ExpireList(null, new TimeSpan(0, 0, 45))));
        }

        [Fact, CleanDatabase]
        public void ExpireList_SetsExpirationTime_OnAList_WithGivenKey()
        {
            var lists = new[]
            {
                new HangfireListItem { Key = "list-1", Value = "1", Position = 0, },
                new HangfireListItem { Key = "list-2", Value = "1", Position = 0, },
            };
            UseContextWithSavingChanges(context => context.Lists.AddRange(lists));

            UseTransaction(transaction => transaction.ExpireList("list-1", new TimeSpan(1, 0, 0)));

            var records = UseContext(context => context.Lists.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.True(DateTime.UtcNow.AddMinutes(59) < records["list-1"]);
            Assert.True(records["list-1"] < DateTime.UtcNow.AddMinutes(61));
            Assert.Null(records["list-2"]);
        }

        [Fact, CleanDatabase]
        public void PersistHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("key",
                () => transaction.PersistHash(null)));
        }

        [Fact, CleanDatabase]
        public void PersistHash_ClearsExpirationTime_OnAGivenHash()
        {
            var hashes = new[]
            {
                new HangfireHash { Key = "hash-1", Field = "field", ExpireAt = DateTime.UtcNow.AddDays(1), },
                new HangfireHash { Key = "hash-2", Field = "field", ExpireAt = DateTime.UtcNow.AddDays(1), },
            };
            UseContextWithSavingChanges(context => context.Hashes.AddRange(hashes));

            UseTransaction(transaction => transaction.PersistHash("hash-1"));

            var records = UseContext(context => context.Hashes.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.Null(records["hash-1"]);
            Assert.NotNull(records["hash-2"]);
        }

        [Fact, CleanDatabase]
        public void PersistSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("key",
                () => transaction.PersistSet(null)));
        }

        [Fact, CleanDatabase]
        public void PersistSet_ClearsExpirationTime_OnAGivenHash()
        {
            var sets = new[]
            {
                new HangfireSet { Key = "set-1", Value = "1", CreatedAt = DateTime.UtcNow, ExpireAt = DateTime.UtcNow.AddDays(1), },
                new HangfireSet { Key = "set-2", Value = "1", CreatedAt = DateTime.UtcNow, ExpireAt = DateTime.UtcNow.AddDays(1), },
            };
            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            UseTransaction(transaction => transaction.PersistSet("set-1"));

            var records = UseContext(context => context.Sets.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.Null(records["set-1"]);
            Assert.NotNull(records["set-2"]);
        }

        [Fact, CleanDatabase]
        public void PersistList_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction => Assert.Throws<ArgumentNullException>("key",
                () => transaction.PersistList(null)));
        }

        [Fact, CleanDatabase]
        public void PersistList_ClearsExpirationTime_OnAGivenHash()
        {
            var lists = new[]
            {
                new HangfireListItem { Key = "list-1", Value = "1", Position = 0, ExpireAt = DateTime.UtcNow.AddDays(1), },
                new HangfireListItem { Key = "list-2", Value = "1", Position = 0, ExpireAt = DateTime.UtcNow.AddDays(1), },
            };
            UseContextWithSavingChanges(context => context.Lists.AddRange(lists));

            UseTransaction(transaction => transaction.PersistList("list-1"));

            var records = UseContext(context => context.Lists.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.Null(records["list-1"]);
            Assert.NotNull(records["list-2"]);
        }

        private Guid InsertTestJob(DateTime? expireAt = null)
        {
            Guid jobId = Guid.NewGuid();
            UseContextWithSavingChanges(context => context.Jobs.
                Add(new HangfireJob { Id = jobId, CreatedAt = DateTime.UtcNow, InvocationData = string.Empty, ExpireAt = expireAt }));

            return jobId;
        }

        private HangfireJob GetTestJob(Guid jobId) => UseContext(context => context.Jobs.
            Include(p => p.ActualState.State).
            Include(p => p.Parameters).
            Include(p => p.States).
            Single(x => x.Id == jobId));

        private void UseTransaction(Action<EntityFrameworkJobStorageTransaction> action)
        {
            string connectionString = ConnectionUtils.GetConnectionString();
            var storage = new EntityFrameworkJobStorage(connectionString);

            using (var transaction = new EntityFrameworkJobStorageTransaction(storage))
            {
                action(transaction);
                transaction.Commit();
            }
        }

        private void UseContext(Action<HangfireDbContext> action)
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            storage.UseHangfireDbContext(action);
        }

        private T UseContext<T>(Func<HangfireDbContext, T> func)
        {
            T result = default(T);
            UseContext(context => { result = func(context); });
            return result;
        }

        private void UseContextWithSavingChanges(Action<HangfireDbContext> action)
        {
            var storage = new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString());
            storage.UseHangfireDbContext(context =>
            {
                action(context);
                context.SaveChanges();
            });
        }
    }
}
