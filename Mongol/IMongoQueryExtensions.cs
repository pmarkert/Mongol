using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Mongol {
	public static class IMongoQueryExtensions {
		public static IMongoQuery And(this IMongoQuery left, IMongoQuery right) {
			return Query.And(left, right);
		}

		public static IMongoQuery Or(this IMongoQuery left, IMongoQuery right) {
			return Query.Or(left, right);
		}
	}
}
