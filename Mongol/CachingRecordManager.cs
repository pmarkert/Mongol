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
using Common.Logging;

namespace Mongol {
	/// <summary>
	/// Works like a standard RecordManager, except that it keeps a cache of all retrieved records.  Well suited for lookups collections
	/// that have a relatively small number of objects that are frequently retrieved for read-only activity.  
	/// </summary>
	/// <remarks>Calling Save() will in addition to saving the object, also update the item in the cache. Calling Delete() will also remove the item from the cache.</remarks>
	public class CachingRecordManager<T> : RecordManager<T> where T : class {
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private static Dictionary<object, T> cache = new Dictionary<object, T>();

		/// <summary>
		/// Retrieves an object by Id and caches the retrieval by Id for future lookup.
		/// </summary>
		public override T GetById(object Id) {
			if (!cache.ContainsKey(Id)) {
				lock (cache) {
					if (!cache.ContainsKey(Id)) {
						logger.Debug(m => m("Cache miss on GetById({0}). performing lookup.", Id));
						var result = base.GetById(Id);
						if (result == null) {
							return null;
						}
						cache.Add(Id, result);
					}
				}
			}
			return cache[Id];
		}

		/// <summary>
		/// Removes all items from the cache.
		/// </summary>
		public void ClearCache() {
			logger.Debug(m => m("ClearCache()"));
			lock (cache) {
				cache.Clear();
			}
		}

		/// <summary>
		/// Removes a single item from the cache.
		/// </summary>
		public void ClearItem(object Id) {
			logger.Debug(m => m("ClearItem({0})", Id));
			lock (cache) {
				if(cache.ContainsKey(Id)) {
					cache.Remove(Id);
				}
			}
		}

		/// <summary>
		/// Deletes the item in the data store and also removes the item from the cache.
		/// </summary>
		public override void DeleteById(object id) {
			logger.Debug(m => m("DeleteById({0})", id));
			base.DeleteById(id);
			if (cache.ContainsKey(id)) {
				cache.Remove(id);
			}
		}

		/// <summary>
		/// Updates the item in the data store and also updates the item in the cache.
		/// </summary>
		public override bool Save(T record) {
			logger.Debug(m => m("Save({0})", record));
			var wasInsert = base.Save(record);
			lock (cache) {
				cache[GetRecordId(record)] = record;
			}
			return wasInsert;
		}
	}
}
