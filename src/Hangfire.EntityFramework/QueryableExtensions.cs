// Copyright (c) 2017 Sergey Zhigunov.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Hangfire.EntityFramework
{
    internal static class QueryableExtensions
    {
        public static IQueryable<TElement> WhereContains<TElement, TValue>(
            this IQueryable<TElement> source,
            Expression<Func<TElement, TValue>> valueSelector,
            IEnumerable<TValue> values)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            if (valueSelector == null)
                throw new ArgumentNullException(nameof(valueSelector));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            var parameterExpression = valueSelector.Parameters.Single();

            var equals = values.Select(value =>
            {
                Expression<Func<TValue>> x = () => value;
                return Expression.Equal(valueSelector.Body, x.Body);
            });

            var body = equals.Aggregate(
                (accumulate, equal) => Expression.OrElse(accumulate, equal));

            var lambdaExpression =
                Expression.Lambda<Func<TElement, bool>>(body, parameterExpression);

            return source.Where(lambdaExpression);
        }

        private class ReplaceVisitor : ExpressionVisitor
        {
            private Expression OldExpression { get; }

            private Expression NewExpression { get; }

            public ReplaceVisitor(Expression oldExpression, Expression newExpression)
            {
                OldExpression = oldExpression;
                NewExpression = newExpression;
            }

            public override Expression Visit(Expression node)
            {
                return node == OldExpression ? NewExpression : base.Visit(node);
            }
        }
    }
}
