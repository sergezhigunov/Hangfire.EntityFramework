// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Runtime.Serialization;

namespace Hangfire.EntityFramework
{
    [Serializable]
    internal class EntityFrameworkDistributedLockTimeoutException : TimeoutException
    {
        public EntityFrameworkDistributedLockTimeoutException()
        { }

        public EntityFrameworkDistributedLockTimeoutException(string message) : base(message)
        { }

        public EntityFrameworkDistributedLockTimeoutException(string message, Exception innerException) : base(message, innerException)
        { }

        protected EntityFrameworkDistributedLockTimeoutException(SerializationInfo info, StreamingContext context) : base(info, context)
        { }
    }
}