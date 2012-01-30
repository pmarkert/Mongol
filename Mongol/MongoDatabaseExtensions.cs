using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Mongol {
	public static class MongoDatabaseExtensions {
		public static MongoCollection<TDocument> GetCollection<TDocument>(this MongoDatabase db) {
			return db.GetCollection<TDocument>(typeof(TDocument).Name);
		}
	}
}
