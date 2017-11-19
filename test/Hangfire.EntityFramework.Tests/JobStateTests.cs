﻿// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using Xunit;

namespace Hangfire.EntityFramework
{
    public class JobStateTests
    {
        [Fact]
        public void Check_EnumValuesAreCorrect()
        {
            Assert.Equal(0, (int)JobState.Enqueued);
            Assert.Equal(1, (int)JobState.Scheduled);
            Assert.Equal(2, (int)JobState.Processing);
            Assert.Equal(3, (int)JobState.Succeeded);
            Assert.Equal(4, (int)JobState.Failed);
            Assert.Equal(5, (int)JobState.Deleted);
            Assert.Equal(6, (int)JobState.Awaiting);
        }
    }
}
