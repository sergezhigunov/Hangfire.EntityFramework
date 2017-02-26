namespace Hangfire.EntityFramework.Utils
{
    internal class ConnectionUtils
    {
        internal static string GetConnectionString() =>
            "Server=(localdb)\\mssqllocaldb;Database=HangfireTestDatabase;Integrated Security=true;";
    }
}