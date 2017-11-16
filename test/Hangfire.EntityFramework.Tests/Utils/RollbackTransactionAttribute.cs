// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Reflection;
using System.Threading;
using System.Transactions;
using Xunit.Sdk;

namespace Hangfire.EntityFramework.Utils
{
    [AttributeUsage(AttributeTargets.Method)]
    internal sealed class RollbackTransactionAttribute : BeforeAfterTestAttribute
    {
        private static object StaticLock { get; } = new object();

        private IsolationLevel IsolationLevel { get; }

        private TransactionScope TransactionScope { get; set; }

        public RollbackTransactionAttribute()
            : this(IsolationLevel.ReadCommitted)
        {
        }

        public RollbackTransactionAttribute(IsolationLevel isolationLevel)
        {
            IsolationLevel = isolationLevel;
        }

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(StaticLock);

            if (IsolationLevel != IsolationLevel.Unspecified)
            {
                TransactionScope = new TransactionScope(
                    TransactionScopeOption.RequiresNew,
                    new TransactionOptions { IsolationLevel = IsolationLevel });
            }
        }

        public override void After(MethodInfo methodUnderTest)
        {
            try
            {
                TransactionScope?.Dispose();
            }
            finally
            {
                Monitor.Exit(StaticLock);
            }
        }
    }
}