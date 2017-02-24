using System.Reflection;
using System.Threading;
using Xunit.Sdk;

namespace Hangfire.EntityFramework.Utils
{
    internal class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static object StaticLock { get; } = new object();

        private static bool Cleaned { get; set; }

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(StaticLock);

            if (!Cleaned)
            {
                CleanDatabase();
                Cleaned = true;
            }
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Monitor.Exit(StaticLock);
        }

        private static void CleanDatabase()
        {
            using (var context = new HangfireDbContext(ConnectionUtils.GetConnectionString()))
            {
                // TODO: Clean up DB data
            }
        }
    }
}