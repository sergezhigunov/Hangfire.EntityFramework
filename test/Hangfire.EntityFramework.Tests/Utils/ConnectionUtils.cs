namespace Hangfire.EntityFramework.Utils
{
    internal class ConnectionUtils
    {
        internal static string GetConnectionString() =>
            "Server=(localdb)\\mssqllocaldb;Database=Hangfire.EntityFramework.Tests;Integrated Security=true;";
    }
}