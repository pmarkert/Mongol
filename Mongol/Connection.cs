using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System.Configuration;

namespace Mongol {
	public static class Connection {
		static Connection() {
			string mongoURL = ConfigurationManager.AppSettings["Mongol.Url"];
			if (String.IsNullOrEmpty(mongoURL)) {
				throw new ConfigurationErrorsException("Missing AppSetting Mongo.Url");
			}
			MongoUrl mongoUrl = new MongoUrl(mongoURL);
			Instance = MongoServer.Create(mongoUrl).GetDatabase(mongoUrl.DatabaseName);
		}

		public static MongoDatabase Instance {
			get;
			private set;
		}
	}
}
