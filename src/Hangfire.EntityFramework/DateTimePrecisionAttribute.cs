using System;

namespace Hangfire.EntityFramework
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    internal class DateTimePrecisionAttribute : Attribute
    {
        public byte Value { get; }

        public DateTimePrecisionAttribute(byte value)
        {
            Value = value;
        }
    }
}
