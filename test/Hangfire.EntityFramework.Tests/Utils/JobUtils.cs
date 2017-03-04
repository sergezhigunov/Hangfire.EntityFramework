// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq.Expressions;
using Hangfire.Common;
using Hangfire.Storage;

namespace Hangfire.EntityFramework.Utils
{
    internal static class JobUtils
    {
        public static string CreateInvocationData(Expression<Action> methodCall)
        {
            var job = Job.FromExpression(methodCall);
            return CreateInvocationData(job);
        }

        public static string CreateInvocationData(Job job)
        {
            var invocationData = InvocationData.Serialize(job);
            return JobHelper.ToJson(invocationData);
        }
    }
}
