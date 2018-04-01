// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace Hangfire.EntityFramework
{
    internal class EntityFrameworkDistributedLock : IDisposable
    {
        private EntityFrameworkDistributedLockManager Manager { get; }
        private string Resource { get; }
        private TimeSpan Timeout { get; }
        private bool Disposed { get; set; }

        public EntityFrameworkDistributedLock(
            EntityFrameworkDistributedLockManager manager,
            string resource)
        {
            if (manager == null)
                throw new ArgumentNullException(nameof(manager));

            if (resource == null)
                throw new ArgumentNullException(nameof(resource));

            if (resource.Length == 0)
                throw new ArgumentException(ErrorStrings.StringCannotBeEmpty, nameof(resource));

            Manager = manager;
            Resource = resource;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!Disposed)
            {
                if (disposing)
                {
                    Manager.ReleaseDistributedLock(Resource);
                    Disposed = true;
                }
            }
        }
    }
}