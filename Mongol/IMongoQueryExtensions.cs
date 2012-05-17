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
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Mongol {
	/// <summary>
	/// These are convenience extension methods for chaining together multiple Mongo Query Criteria with a fluent syntax.
	/// </summary>
	public static class IMongoQueryExtensions {
		/// <summary>
		/// Joins the two queries with a $and operator
		/// </summary>
		public static IMongoQuery And(this IMongoQuery left, IMongoQuery right) {
			return Query.And(left, right);
		}

		/// <summary>
		/// Joins the two queries with an $or operator
		/// </summary>
		public static IMongoQuery Or(this IMongoQuery left, IMongoQuery right) {
			return Query.Or(left, right);
		}
	}
}
