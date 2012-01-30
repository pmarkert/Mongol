using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mongol;
using System.Linq.Expressions;
using MongoDB.Bson.Serialization.Attributes;

namespace Mongol {
	public static class PropertyNameResolver<T> {
		public static string Resolve<S>(Expression<Func<T, S>> expression) {
			MemberExpression member = expression.Body as MemberExpression;
			if (member == null) {
				return String.Empty;
			}
			else {
				return ResolveRecursiveMemberName(member);
			}
		}

		private static string ResolveRecursiveMemberName(Expression expression) {
			MemberExpression memberExpression = expression as MemberExpression;
			if (memberExpression != null) {
				var prefix = ResolveRecursiveMemberName(memberExpression.Expression);
				if (prefix != String.Empty) {
					return String.Concat(prefix, ".", memberExpression.Member.Name);
				}
				else {
					return memberExpression.Member.Name;
				}
			}
			MethodCallExpression methodCallExpression = expression as MethodCallExpression;
			if (methodCallExpression != null && methodCallExpression.Method.DeclaringType == typeof(System.Linq.Enumerable) && methodCallExpression.Method.Name == "Single") {
				return ResolveRecursiveMemberName(methodCallExpression.Arguments[0]);
			}
			else {
				return String.Empty;
			}
		}
	}
}
