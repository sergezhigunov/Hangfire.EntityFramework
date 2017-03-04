// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

namespace Hangfire.EntityFramework.Utils
{
    internal class ConnectionUtils
    {
        internal static string GetConnectionString() =>
            "Server=(localdb)\\mssqllocaldb;Database=Hangfire.EntityFramework.Tests;Integrated Security=true;";
    }
}