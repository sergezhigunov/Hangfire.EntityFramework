// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.Entity;
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
    public class EntityFrameworkJobStorageTransactionTests
    {
        private static string JobId { get; } =
            Guid.NewGuid().ToString();

        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new EntityFrameworkJobStorageTransaction(null));
        }

        [Fact]
        public void ExpireJob_ThrowsAnException_WhenJobIdIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("jobId",
                    () => transaction.ExpireJob(null, TimeSpan.FromDays(1))));
        }

        [Fact]
        public void ExpireJob_ThrowsAnException_WhenJobIdIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("jobId",
                    () => transaction.ExpireJob(string.Empty, TimeSpan.FromDays(1))));
        }

        [Fact]
        public void ExpireJob_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.ExpireJob(JobId, TimeSpan.FromDays(1)));
        }

        [Fact, RollbackTransaction]
        public void ExpireJob_SetsJobExpirationData()
        {
            var job = InsertTestJob();
            var anotherJob = InsertTestJob();

            UseTransaction(transaction => transaction.ExpireJob(job.Id.ToString(CultureInfo.InvariantCulture), TimeSpan.FromDays(1)));

            var actualJob = GetTestJob(job.Id);
            var approxExpireAt = DateTime.UtcNow.AddDays(1);
            Assert.True(approxExpireAt.AddMinutes(-1) < actualJob.ExpireAt && actualJob.ExpireAt < approxExpireAt.AddMinutes(1));

            anotherJob = GetTestJob(anotherJob.Id);
            Assert.Null(anotherJob.ExpireAt);
        }

        [Fact]
        public void PersistJob_ThrowsAnException_WhenJobIdIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("jobId",
                    () => transaction.PersistJob(null)));
        }

        [Fact]
        public void PersistJob_ThrowsAnException_WhenJobIdIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("jobId",
                    () => transaction.PersistJob(string.Empty)));
        }

        [Fact]
        public void PersistJob_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.PersistJob(JobId));
        }

        [Fact, RollbackTransaction]
        public void PersistJob_ClearsTheJobExpirationData()
        {
            var job = InsertTestJob(DateTime.UtcNow);
            var anotherJob = InsertTestJob(DateTime.UtcNow);

            UseTransaction(transaction => transaction.PersistJob(job.Id.ToString(CultureInfo.InvariantCulture)));

            var actualJob = GetTestJob(job.Id);
            Assert.Null(actualJob.ExpireAt);

            anotherJob = GetTestJob(anotherJob.Id);
            Assert.NotNull(anotherJob.ExpireAt);
        }

        [Fact]
        public void SetJobState_ThrowsAnException_WhenJobIdIsNull()
        {
            var transaction = CreateDisposedTransaction();
            var state = new Mock<IState>();

            Assert.Throws<ArgumentNullException>("jobId",
                () => transaction.SetJobState(null, state.Object));
        }

        [Fact]
        public void SetJobState_ThrowsAnException_WhenJobIdIsEmpty()
        {
            UseTransaction(transaction =>
            {
                var state = new Mock<IState>();

                Assert.Throws<ArgumentException>("jobId",
                    () => transaction.SetJobState(string.Empty, state.Object));
            });
        }

        [Fact]
        public void SetJobState_ThrowsAnException_WhenStateIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("state",
                    () => transaction.SetJobState(JobId, null)));
        }

        [Fact]
        public void SetJobState_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            var state = new Mock<IState>();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.SetJobState(JobId, state.Object));
        }

        [Fact, RollbackTransaction]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            var job = InsertTestJob();
            var anotherJob = InsertTestJob();

            var state = new Mock<IState>();
            state.Setup(x => x.Name).Returns(AwaitingState.StateName);
            state.Setup(x => x.Reason).Returns("Reason");
            state.Setup(x => x.SerializeData()).
                Returns(new Dictionary<string, string>
                {
                    ["Name"] = "Value",
                });

            DateTime beginTimestamp = DateTime.UtcNow.AddSeconds(-1);
            UseTransaction(transaction => transaction.SetJobState(job.Id.ToString(CultureInfo.InvariantCulture), state.Object));
            DateTime endTimestamp = DateTime.UtcNow.AddSeconds(1);

            var actualJob = GetTestJob(job.Id);
            Assert.Equal(AwaitingState.StateName, actualJob.ActualState);

            anotherJob = GetTestJob(anotherJob.Id);
            Assert.Null(anotherJob.ActualState);

            var jobState = GetTestJobActualState(job.Id);
            Assert.Equal(AwaitingState.StateName, jobState.Name);
            Assert.Equal("Reason", jobState.Reason);
            Assert.True(beginTimestamp <= jobState.CreatedAt && jobState.CreatedAt <= endTimestamp);
            var data = JobHelper.FromJson<Dictionary<string, string>>(jobState.Data);
            Assert.Single(data);
            Assert.Equal("Value", data["Name"]);
        }

        [Fact]
        public void AddJobState_ThrowsAnException_WhenJobIdIsNull()
        {
            UseTransaction(transaction =>
            {
                var state = new Mock<IState>();

                Assert.Throws<ArgumentNullException>("jobId",
                    () => transaction.AddJobState(null, state.Object));
            });
        }

        [Fact]
        public void AddJobState_ThrowsAnException_WhenJobIdIsEmpty()
        {
            UseTransaction(transaction =>
            {
                var state = new Mock<IState>();

                Assert.Throws<ArgumentException>("jobId",
                    () => transaction.AddJobState(string.Empty, state.Object));
            });
        }

        [Fact]
        public void AddJobState_ThrowsAnException_WhenStateIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("state",
                    () => transaction.AddJobState(JobId, null)));
        }

        [Fact]
        public void AddJobState_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            var state = new Mock<IState>();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.SetJobState(JobId, state.Object));
        }

        [Fact, RollbackTransaction]
        public void AddJobState_JustAddsANewRecordInATable()
        {
            var job = InsertTestJob();

            var state = new Mock<IState>();
            state.Setup(x => x.Name).Returns(AwaitingState.StateName);
            state.Setup(x => x.Reason).Returns("Reason");
            state.Setup(x => x.SerializeData()).
                Returns(new Dictionary<string, string>
                {
                    ["Name"] = "Value",
                });

            DateTime beginTimestamp = DateTime.UtcNow.AddSeconds(-1);
            UseTransaction(transaction => transaction.AddJobState(job.Id.ToString(CultureInfo.InvariantCulture), state.Object));
            DateTime endTimestamp = DateTime.UtcNow.AddSeconds(1);

            var actualJob = GetTestJob(job.Id);
            Assert.Null(actualJob.ActualState);

            var jobState = Assert.Single(actualJob.States);
            Assert.Equal(AwaitingState.StateName, jobState.Name);
            Assert.Equal("Reason", jobState.Reason);
            Assert.True(beginTimestamp <= jobState.CreatedAt && jobState.CreatedAt <= endTimestamp);
            var data = JobHelper.FromJson<Dictionary<string, string>>(jobState.Data);
            Assert.Single(data);
            Assert.Equal("Value", data["Name"]);
        }

        [Fact]
        public void AddToQueue_ThrowsAnException_WhenJobIdIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("jobId",
                    () => transaction.AddToQueue("QUEUE", null)));
        }

        [Fact]
        public void AddToQueue_ThrowsAnException_WhenJobIdIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("jobId",
                    () => transaction.AddToQueue("QUEUE", string.Empty)));
        }

        [Fact]
        public void AddToQueue_ThrowsAnException_WhenQueueIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("queue",
                    () => transaction.AddToQueue(null, JobId)));
        }

        [Fact]
        public void AddToQueue_ThrowsAnException_WhenQueueIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("queue",
                    () => transaction.AddToQueue(string.Empty, JobId)));
        }

        [Fact]
        public void AddToQueue_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.AddToQueue("QUEUE", JobId));
        }

        [Fact, RollbackTransaction]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            var job = InsertTestJob();

            UseTransaction(transaction => transaction.AddToQueue("DEFAULT", job.Id.ToString(CultureInfo.InvariantCulture)));

            var result = UseContext(context => context.JobQueues.Single());

            Assert.Equal(job.Id, result.JobId);
            Assert.Equal("DEFAULT", result.Queue);
        }

        [Fact]
        public void IncrementCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.IncrementCounter(null)));
        }

        [Fact]
        public void IncrementCounter_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.IncrementCounter(string.Empty)));
        }

        [Fact]
        public void IncrementCounter_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.IncrementCounter(key));
        }

        [Fact, RollbackTransaction]
        public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.IncrementCounter(key));

            var record = UseContext(context => context.Counters.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal(1, record.Value);
            Assert.Null(record.ExpireAt);
        }

        [Fact]
        public void IncrementCounter_WithExpiry_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.IncrementCounter(null, TimeSpan.FromDays(1))));
        }

        [Fact]
        public void IncrementCounter_WithExpiry_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.IncrementCounter(string.Empty, TimeSpan.FromDays(1))));
        }

        [Fact]
        public void IncrementCounter_WithExpiry_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.IncrementCounter(key, TimeSpan.FromDays(1)));
        }

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
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

        [Fact]
        public void DecrementCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.DecrementCounter(null)));
        }

        [Fact]
        public void DecrementCounter_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.DecrementCounter(string.Empty)));
        }

        [Fact]
        public void DecrementCounter_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.DecrementCounter(key));
        }

        [Fact, RollbackTransaction]
        public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.DecrementCounter(key));

            var record = UseContext(context => context.Counters.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal(-1, record.Value);
            Assert.Null(record.ExpireAt);
        }

        [Fact]
        public void DecrementCounter_WithExpiry_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.DecrementCounter(null, TimeSpan.FromDays(1))));
        }

        [Fact]
        public void DecrementCounter_WithExpiry_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.DecrementCounter(string.Empty, TimeSpan.FromDays(1))));
        }

        [Fact]
        public void DecrementCounter_WithExpiry_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.DecrementCounter(key, TimeSpan.FromDays(1)));
        }

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
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

        [Fact]
        public void AddToSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.AddToSet(null, "my-value")));
        }

        [Fact]
        public void AddToSet_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.AddToSet(string.Empty, "my-value")));
        }

        [Fact]
        public void AddToSet_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.AddToSet(key, "my-value"));
        }

        [Fact, RollbackTransaction]
        public void AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.AddToSet(key, "my-value"));

            var record = UseContext(context => context.Sets.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal("my-value", record.Value);
            Assert.Equal(0.0, record.Score, 2);
        }

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
        public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.AddToSet(key, "my-value", 3.2));

            var record = UseContext(context => context.Sets.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal("my-value", record.Value);
            Assert.Equal(3.2, record.Score, 3);
        }

        [Fact, RollbackTransaction]
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

        [Fact]
        public void RemoveFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.RemoveFromSet(null, "my-value")));
        }

        [Fact]
        public void RemoveFromSet_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.RemoveFromSet(string.Empty, "my-value")));
        }

        [Fact]
        public void RemoveFromSet_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.RemoveFromSet(key, "my-value"));
        }

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
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

        [Fact]
        public void InsertToList_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.InsertToList(null, "my-value")));
        }

        [Fact]
        public void InsertToList_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.InsertToList(string.Empty, "my-value")));
        }

        [Fact]
        public void InsertToList_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.InsertToList(key, "my-value"));
        }

        [Fact, RollbackTransaction]
        public void InsertToList_AddsARecord_WithGivenValues()
        {
            string key = Guid.NewGuid().ToString();

            UseTransaction(transaction => transaction.InsertToList(key, "my-value"));

            var record = UseContext(context => context.Lists.Single());

            Assert.Equal(key, record.Key);
            Assert.Equal("my-value", record.Value);
        }

        [Fact, RollbackTransaction]
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

        [Fact]
        public void RemoveFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.RemoveFromList(null, "my-value")));
        }

        [Fact]
        public void RemoveFromList_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.RemoveFromList(string.Empty, "my-value")));
        }

        [Fact]
        public void RemoveFromList_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.RemoveFromList(key, "my-value"));
        }

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
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

        [Fact]
        public void TrimList_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.TrimList(null, 0, 1)));
        }

        [Fact]
        public void TrimList_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.TrimList(string.Empty, 0, 1)));
        }

        [Fact]
        public void TrimList_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.TrimList(key, 0, 1));
        }

        [Fact, RollbackTransaction]
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

        [Fact, RollbackTransaction]
        public void TrimList_RemovesRecordsToEnd_IfKeepAndingAt_GreaterThanMaxElementIndex()
        {
            string key = Guid.NewGuid().ToString();
            UseContextWithSavingChanges(context => context.Lists.AddRange(new[]
            {
                new HangfireList
                {
                    Key = key,
                    Position = 0,
                    Value = "0",
                },
                new HangfireList
                {
                    Key = key,
                    Position = 1,
                    Value = "1",
                },
                new HangfireList
                {
                    Key = key,
                    Position = 2,
                    Value = "2",
                },
            }));

            UseTransaction(transaction => transaction.TrimList(key, 1, 100));

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(2, recordCount);
        }

        [Fact, RollbackTransaction]
        public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
        {
            string key = Guid.NewGuid().ToString();
            UseContextWithSavingChanges(context => context.Lists.
                Add(new HangfireList
                {
                    Key = key,
                    Position = 0,
                    Value = "0",
                }));

            UseTransaction(transaction => transaction.TrimList(key, 1, 100));

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(0, recordCount);
        }

        [Fact, RollbackTransaction]
        public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
        {
            string key = Guid.NewGuid().ToString();
            UseContextWithSavingChanges(context => context.Lists.
                Add(new HangfireList
                {
                    Key = key,
                    Position = 0,
                    Value = "0",
                }));

            UseTransaction(transaction => transaction.TrimList(key, 1, 0));

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(0, recordCount);
        }

        [Fact, RollbackTransaction]
        public void TrimList_RemovesRecords_OnlyOfAGivenKey()
        {
            string key = Guid.NewGuid().ToString();
            string anotherKey = Guid.NewGuid().ToString();
            UseContextWithSavingChanges(context => context.Lists.
                Add(new HangfireList
                {
                    Key = key,
                    Position = 0,
                    Value = "0",
                }));

            UseTransaction(transaction => transaction.TrimList(anotherKey, 1, 0));

            var recordCount = UseContext(context => context.Lists.Count());

            Assert.Equal(1, recordCount);
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.SetRangeInHash(key, new Dictionary<string, string>()));
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.SetRangeInHash(null, new Dictionary<string, string>())));
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsIsNull()
        {
            UseTransaction(transaction =>
            {
                var key = Guid.NewGuid().ToString();

                Assert.Throws<ArgumentNullException>("keyValuePairs",
                    () => transaction.SetRangeInHash(key, null));
            });
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.SetRangeInHash(string.Empty, new Dictionary<string, string>())));
        }

        [Fact, RollbackTransaction]
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

        [Fact]
        public void RemoveHash_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.RemoveHash(key));
        }

        [Fact]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.RemoveHash(null)));
        }

        [Fact]
        public void RemoveHash_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.RemoveHash(string.Empty)));
        }

        [Fact, RollbackTransaction]
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

        [Fact]
        public void AddRangeToSet_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.AddRangeToSet(key, new List<string>()));
        }

        [Fact]
        public void AddRangeToSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.AddRangeToSet(null, new List<string>())));
        }

        [Fact]
        public void AddRangeToSet_ThrowsAnException_WhenItemsIsNull()
        {
            UseTransaction(transaction =>
            {
                string key = Guid.NewGuid().ToString();

                Assert.Throws<ArgumentNullException>("items",
                    () => transaction.AddRangeToSet(key, null));
            });
        }

        [Fact]
        public void AddRangeToSet_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.AddRangeToSet(string.Empty, new List<string>())));
        }

        [Fact, RollbackTransaction]
        public void AddRangeToSet_AddsAllItems_ToAGivenSet()
        {
            string setId = Guid.NewGuid().ToString();
            var items = new List<string>
            {
                "1",
                "2",
                "3",
            };

            UseTransaction(transaction => transaction.AddRangeToSet(setId, items));

            var records = UseContext(context => (
                from set in context.Sets
                where set.Key == setId
                select set.Value).
                ToArray());

            Assert.Equal(items, records);
        }

        [Fact]
        public void RemoveSet_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.RemoveSet(key));
        }

        [Fact]
        public void RemoveSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.RemoveSet(null)));
        }

        [Fact]
        public void RemoveSet_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.RemoveSet(string.Empty)));
        }

        [Fact, RollbackTransaction]
        public void RemoveSet_RemovesASet_WithAGivenKey()
        {
            var sets = new[]
            {
                new HangfireSet
                {
                    Key = "set-1",
                    Value = "1",
                    CreatedAt = DateTime.UtcNow,
                },
                new HangfireSet
                {
                    Key = "set-2",
                    Value = "1",
                    CreatedAt = DateTime.UtcNow,
                },
            };
            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            UseTransaction(transaction => transaction.RemoveSet("set-1"));

            var record = UseContext(context => context.Sets.Single());
            Assert.Equal("set-2", record.Key);
        }

        [Fact]
        public void ExpireHash_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.ExpireHash(key, TimeSpan.FromMinutes(5)));
        }

        [Fact]
        public void ExpireHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.ExpireHash(null, TimeSpan.FromMinutes(5))));
        }

        [Fact]
        public void ExpireHash_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.ExpireHash(string.Empty, TimeSpan.FromMinutes(5))));
        }

        [Fact, RollbackTransaction]
        public void ExpireHash_SetsExpirationTimeOnAHash_WithGivenKey()
        {
            var hashes = new[]
            {
                new HangfireHash
                {
                    Key = "hash-1",
                    Field = "field",
                },
                new HangfireHash
                {
                    Key = "hash-2",
                    Field = "field",
                },
            };

            UseContextWithSavingChanges(context => context.Hashes.AddRange(hashes));

            UseTransaction(transaction => transaction.ExpireHash("hash-1", new TimeSpan(1, 0, 0)));

            var records = UseContext(context => context.Hashes.ToDictionary(x => x.Key, x => x.ExpireAt));

            Assert.True(DateTime.UtcNow.AddMinutes(59) < records["hash-1"]);
            Assert.True(records["hash-1"] < DateTime.UtcNow.AddMinutes(61));
            Assert.Null(records["hash-2"]);
        }

        [Fact]
        public void ExpireSet_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.ExpireSet(key, new TimeSpan(0, 0, 45)));
        }

        [Fact]
        public void ExpireSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.ExpireSet(null, new TimeSpan(0, 0, 45))));
        }

        [Fact]
        public void ExpireSet_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.ExpireSet(string.Empty, new TimeSpan(0, 0, 45))));
        }

        [Fact, RollbackTransaction]
        public void ExpireSet_SetsExpirationTime_OnASet_WithGivenKey()
        {
            var sets = new[]
            {
                new HangfireSet
                {
                    Key = "set-1",
                    Value = "1",
                    CreatedAt = DateTime.UtcNow,
                },
                new HangfireSet
                {
                    Key = "set-2",
                    Value = "1",
                    CreatedAt = DateTime.UtcNow,
                },
            };

            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            UseTransaction(transaction => transaction.ExpireSet("set-1", new TimeSpan(1, 0, 0)));

            var records = UseContext(context => context.Sets.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.True(DateTime.UtcNow.AddMinutes(59) < records["set-1"]);
            Assert.True(records["set-1"] < DateTime.UtcNow.AddMinutes(61));
            Assert.Null(records["set-2"]);
        }

        [Fact]
        public void ExpireList_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.ExpireList(key, new TimeSpan(0, 0, 45)));
        }

        [Fact]
        public void ExpireList_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.ExpireList(null, new TimeSpan(0, 0, 45))));
        }

        [Fact]
        public void ExpireList_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.ExpireList(string.Empty, new TimeSpan(0, 0, 45))));
        }

        [Fact, RollbackTransaction]
        public void ExpireList_SetsExpirationTime_OnAList_WithGivenKey()
        {
            var lists = new[]
            {
                new HangfireList
                {
                    Key = "list-1",
                    Value = "1",
                    Position = 0,
                },
                new HangfireList
                {
                    Key = "list-2",
                    Value = "1",
                    Position = 0,
                },
            };

            UseContextWithSavingChanges(context => context.Lists.AddRange(lists));

            UseTransaction(transaction => transaction.ExpireList("list-1", new TimeSpan(1, 0, 0)));

            var records = UseContext(context => context.Lists.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.True(DateTime.UtcNow.AddMinutes(59) < records["list-1"]);
            Assert.True(records["list-1"] < DateTime.UtcNow.AddMinutes(61));
            Assert.Null(records["list-2"]);
        }

        [Fact]
        public void PersistHash_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.PersistHash(key));
        }

        [Fact]
        public void PersistHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.PersistHash(null)));
        }

        [Fact]
        public void PersistHash_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.PersistHash(string.Empty)));
        }

        [Fact, RollbackTransaction]
        public void PersistHash_ClearsExpirationTime_OnAGivenHash()
        {
            var hashes = new[]
            {
                new HangfireHash
                {
                    Key = "hash-1",
                    Field = "field",
                    ExpireAt = DateTime.UtcNow.AddDays(1)
                },
                new HangfireHash
                {
                    Key = "hash-2",
                    Field = "field",
                    ExpireAt = DateTime.UtcNow.AddDays(1),
                },
            };
            UseContextWithSavingChanges(context => context.Hashes.AddRange(hashes));

            UseTransaction(transaction => transaction.PersistHash("hash-1"));

            var records = UseContext(context => context.Hashes.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.Null(records["hash-1"]);
            Assert.NotNull(records["hash-2"]);
        }

        [Fact]
        public void PersistSet_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.PersistSet(key));
        }

        [Fact]
        public void PersistSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.PersistSet(null)));
        }

        [Fact]
        public void PersistSet_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.PersistSet(string.Empty)));
        }

        [Fact, RollbackTransaction]
        public void PersistSet_ClearsExpirationTime_OnAGivenHash()
        {
            var sets = new[]
            {
                new HangfireSet
                {
                    Key = "set-1",
                    Value = "1",
                    CreatedAt = DateTime.UtcNow,
                    ExpireAt = DateTime.UtcNow.AddDays(1),
                },
                new HangfireSet
                {
                    Key = "set-2",
                    Value = "1",
                    CreatedAt = DateTime.UtcNow,
                    ExpireAt = DateTime.UtcNow.AddDays(1),
                },
            };

            UseContextWithSavingChanges(context => context.Sets.AddRange(sets));

            UseTransaction(transaction => transaction.PersistSet("set-1"));

            var records = UseContext(context => context.Sets.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.Null(records["set-1"]);
            Assert.NotNull(records["set-2"]);
        }

        [Fact]
        public void PersistList_ThrowsAnException_WhenTransactionDisposed()
        {
            var transaction = CreateDisposedTransaction();
            string key = Guid.NewGuid().ToString();

            Assert.Throws<ObjectDisposedException>(
                () => transaction.PersistList(key));
        }

        [Fact]
        public void PersistList_ThrowsAnException_WhenKeyIsNull()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentNullException>("key",
                    () => transaction.PersistList(null)));
        }

        [Fact]
        public void PersistList_ThrowsAnException_WhenKeyIsEmpty()
        {
            UseTransaction(transaction =>
                Assert.Throws<ArgumentException>("key",
                    () => transaction.PersistList(string.Empty)));
        }

        [Fact, RollbackTransaction]
        public void PersistList_ClearsExpirationTime_OnAGivenHash()
        {
            var lists = new[]
            {
                new HangfireList
                {
                    Key = "list-1",
                    Value = "1",
                    Position = 0,
                    ExpireAt = DateTime.UtcNow.AddDays(1),
                },
                new HangfireList
                {
                    Key = "list-2",
                    Value = "1",
                    Position = 0,
                    ExpireAt = DateTime.UtcNow.AddDays(1),
                },
            };
            UseContextWithSavingChanges(context => context.Lists.AddRange(lists));

            UseTransaction(transaction => transaction.PersistList("list-1"));

            var records = UseContext(context => context.Lists.ToDictionary(x => x.Key, x => x.ExpireAt));
            Assert.Null(records["list-1"]);
            Assert.NotNull(records["list-2"]);
        }

        private HangfireJob InsertTestJob(DateTime? expireAt = null)
        {
            var job = UseContextWithSavingChanges(context => context.Jobs.
                Add(new HangfireJob
                {
                    CreatedAt = DateTime.UtcNow,
                    ExpireAt = expireAt,
                }));

            return job;
        }

        private HangfireJob GetTestJob(long jobId) => UseContext(context => context.Jobs.
            Include(p => p.Parameters).
            Include(p => p.States).
            Single(x => x.Id == jobId));

        private HangfireJobState GetTestJobActualState(long jobId) => UseContext(context => (
            from state in context.JobStates
            where state.JobId == jobId && state.Job.ActualState == state.Name
            orderby state.CreatedAt descending
            select state).
            FirstOrDefault());

        private static EntityFrameworkJobStorageTransaction CreateDisposedTransaction()
        {
            var transaction = CreateTransaction();
            transaction.Dispose();
            return transaction;
        }
    }
}
