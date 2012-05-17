/* Copyright 2012 Ephisys Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
   limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mongol;
using System.Linq.Expressions;
using MongoDB.Bson.Serialization.Attributes;

namespace Mongol {
	/// <summary>
	/// The PropertyNameResolver static class provides utility methods to generate strings representing field-member access for use in MongoDB query and update operations.
	/// This allows you to use lambda expressions to represent the properties you wish to use for query or update instead of coding literal string values to represent those members.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public static class PropertyNameResolver<T> {
		/// <summary>
		/// Resolves a member-access expression into a dotted-string suitable for using in a MongoDB query or update statement.
		/// </summary>
		/// <typeparam name="S"></typeparam>
		/// <param name="expression"></param>
		/// <returns></returns>
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
			if (methodCallExpression != null && ((methodCallExpression.Method.DeclaringType == typeof(System.Linq.Enumerable) && (methodCallExpression.Method.Name == "Single")) || methodCallExpression.Method.Name == "Member")) {
				return ResolveRecursiveMemberName(methodCallExpression.Arguments[0]);
			}
			else {
				return String.Empty;
			}
		}
	}
}
