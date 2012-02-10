using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mongol {
	/// <summary>
	/// Works like a standard RecordManager, except that it keeps a cache of all retrieved records.  Well suited for lookups collections
	/// that have a relatively small number of objects that a frequently retrieved for read-only activity.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class CachingRecordManager<T> : RecordManager<T> where T : Record {
		private static Dictionary<string, T> cache = new Dictionary<string, T>();

		public override T GetById(string Id) {
			if (!cache.ContainsKey(Id)) {
				lock (cache) {
					if (!cache.ContainsKey(Id)) {
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

		public void ClearCache() {
			lock (cache) {
				cache.Clear();
			}
		}

		public override bool Save(T record) {
			var wasInsert = base.Save(record);
			lock (cache) {
				cache[record.Id] = record;
			}
			return wasInsert;
		}
	}
}
