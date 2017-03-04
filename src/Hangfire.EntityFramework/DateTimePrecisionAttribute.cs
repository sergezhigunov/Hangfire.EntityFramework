// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

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
