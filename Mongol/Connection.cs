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
using System.Linq.Expressions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System.Configuration;
using System.IO;
using System.Threading;
using Common.Logging;

namespace Mongol {
	/// <summary>
	/// This class coordinates the connection strings for Mongol.  By default it will read connection strings from the appSettings elements.  &quot;Mongol.Url&quot; is the key for the default connection.
	/// Specific named connections can be specified in appSettings as &quot;Mongol.Url.CONNECTIONNAME&quot;.  Alternatively, connections can be explicitly configured by the application by calling AddConnection.
	/// </summary>
	public static class Connection {
		private const string appSettingPrefix = "Mongol.Url";

		private static readonly ILog logger = LogManager.GetCurrentClassLogger();
		private static Dictionary<string, MongoUrl> connections;
		private static ReaderWriterLockSlim rwl = new ReaderWriterLockSlim();

		static Connection() {
			connections = new Dictionary<string, MongoUrl>();
			foreach (String key in ConfigurationManager.AppSettings.Keys) {
				if (key.StartsWith(appSettingPrefix)) {
					if (key.Equals(appSettingPrefix)) {
						logger.Debug(m => m("Initialized Mongol Connection:[default] - {0}",ConfigurationManager.AppSettings[key]));
						connections.Add(String.Empty, new MongoUrl(ConfigurationManager.AppSettings[key]));
					}
					else {
						if (key.StartsWith(appSettingPrefix + ".")) {
							string connectionName = key.Substring(appSettingPrefix.Length + 1);
							logger.Debug(m => m("Initialized Mongol Connection:{0} - {1}", connectionName, ConfigurationManager.AppSettings[key]));
							connections.Add(connectionName, new MongoUrl(ConfigurationManager.AppSettings[key]));
						}
					}
				}
			}
		}

		/// <summary>
		/// Retrieves a MongoDatabase instance based upon the named connection.
		/// </summary>
		/// <param name="Name">The name of the connection (null if default connection)</param>
		public static MongoDatabase GetInstance(string Name = null) {
			MongoUrl mongoUrl = GetMongolUrlByName(Name ?? String.Empty);
			return MongoServer.Create(mongoUrl).GetDatabase(mongoUrl.DatabaseName);
		}

		/// <summary>
		/// Sets the value for a named connection.
		/// </summary>
		/// <param name="Name">The name of the new connection.</param>
		/// <param name="url">The MongoDB url for the connection.  Pass a value of null to remove the connection from the list.</param>
		public static void SetConnection(string Name, string url) {
			logger.Debug(m => m("SetConnection({0},{1})", Name, url));
			rwl.EnterWriteLock();
			try {
				if (String.IsNullOrEmpty(url) && connections.ContainsKey(Name)) {
					connections.Remove(Name);
				}
				else {
					connections[Name ?? String.Empty] = new MongoUrl(url);
				}
			}
			finally {
				rwl.ExitWriteLock();
			}
		}

		private static MongoUrl GetMongolUrlByName(string Name) {
			rwl.EnterReadLock();
			try {
				string suffix = String.IsNullOrEmpty(Name) ? null : suffix = "." + Name;
				if (!connections.ContainsKey(Name)) {
					throw new ConfigurationErrorsException("Missing AppSetting Mongol.Url" + suffix);
				}
				return connections[Name];
			}
			finally {
				rwl.ExitReadLock();
			}
		}
	}
}
