// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using System.Threading;
using Hangfire.EntityFramework.Utils;
using Xunit;

namespace Hangfire.EntityFramework
{
    public class CountersAggregatorTests
    {
        private TimeSpan AggregationInterval { get; } = new TimeSpan(1);

        [Fact]
        public void Ctor_ThrowsException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new CountersAggregator(null, AggregationInterval));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void Ctor_ThrowsException_WhenAggregationIntervalIsNonPositive(int seconds)
        {
            var storage = CreateStorage();

            Assert.Throws<ArgumentOutOfRangeException>("aggregationInterval",
                () => new CountersAggregator(storage, new TimeSpan(0, 0, seconds)));
        }

        [Fact, CleanDatabase]
        public void Execute_DoWorkCorrectly()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 10; i++)
                    context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = "counter1", Value = 1 });
                for (int i = 0; i < 20; i++)
                    context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = "counter2", Value = -1 });
                for (int i = 0; i < 5; i++)
                    context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = "counter3", Value = 20 });
                context.Counters.Add(new HangfireCounter { Id = Guid.NewGuid(), Key = "counter3", Value = -1 });
            });
            var storage = CreateStorage();
            var aggregator = new CountersAggregator(storage, AggregationInterval);
            var cts = new CancellationTokenSource();

            aggregator.Execute(cts.Token);

            var result = UseContext(context => context.Counters.ToArray());

            Assert.Equal(3, result.Length);
            Assert.Equal(10, result.Single(x => x.Key == "counter1").Value);
            Assert.Equal(-20, result.Single(x => x.Key == "counter2").Value);
            Assert.Equal(99, result.Single(x => x.Key == "counter3").Value);
        }

        private T UseContext<T>(Func<HangfireDbContext, T> func)
        {
            T result = default(T);
            UseContext(context => { result = func(context); });
            return result;
        }

        private void UseContext(Action<HangfireDbContext> action)
        {
            var storage = CreateStorage();
            storage.UseHangfireDbContext(action);
        }

        private void UseContextWithSavingChanges(Action<HangfireDbContext> action)
        {
            var storage = CreateStorage();
            storage.UseHangfireDbContext(context =>
            {
                action(context);
                context.SaveChanges();
            });
        }

        private EntityFrameworkJobStorage CreateStorage() =>
            new EntityFrameworkJobStorage(ConnectionUtils.GetConnectionString(), new EntityFrameworkJobStorageOptions());
    }
}
