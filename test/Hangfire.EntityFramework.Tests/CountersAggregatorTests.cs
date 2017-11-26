// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.EntityFramework.Utils;
using Hangfire.Server;
using Xunit;

namespace Hangfire.EntityFramework
{
    using static ConnectionUtils;

    [CleanDatabase]
    public class CountersAggregatorTests
    {
        private CancellationTokenSource CancellationTokenSource { get; } =
            new CancellationTokenSource();

        private TimeSpan AggregationInterval { get; } =
            new TimeSpan(1);

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

        [Fact, RollbackTransaction]
        public void Execute_DoWorkCorrectly()
        {
            UseContextWithSavingChanges(context =>
            {
                for (int i = 0; i < 10; i++)
                    context.Counters.Add(new HangfireCounter
                    {
                        Key = "counter1",
                        Value = 1
                    });

                for (int i = 0; i < 20; i++)
                    context.Counters.Add(new HangfireCounter
                    {
                        Key = "counter2",
                        Value = -1
                    });

                for (int i = 0; i < 5; i++)
                    context.Counters.Add(new HangfireCounter
                    {
                        Key = "counter3",
                        Value = 20
                    });

                context.Counters.Add(new HangfireCounter
                {
                    Key = "counter3",
                    Value = -1
                });
            });

            var storage = CreateStorage();
            var aggregator = new CountersAggregator(storage, AggregationInterval);
            var processContext = CreateProcessContext();

            aggregator.Execute(processContext);

            var result = UseContext(context => context.Counters.ToArray());

            Assert.Equal(3, result.Length);
            Assert.Equal(10, result.Single(x => x.Key == "counter1").Value);
            Assert.Equal(-20, result.Single(x => x.Key == "counter2").Value);
            Assert.Equal(99, result.Single(x => x.Key == "counter3").Value);
        }

        private BackgroundProcessContext CreateProcessContext()
        {
            return new BackgroundProcessContext(
                "server", CreateStorage(),
                new Dictionary<string, object>(),
                CancellationTokenSource.Token);
        }
    }
}
