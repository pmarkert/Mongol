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

namespace Mongol {
	/// <summary>
	/// Simple instance cache for RecordManagers.
	/// </summary>
	public class ManagerFactory {

		private static Dictionary<Type, IRecordManager> _managers = new Dictionary<Type, IRecordManager>();

		/// <summary>
		/// Retrieves an instance of the specified RecordManager from an internal cache.  Creates a new instance using the default constructor if one doesn't already exist.
		/// </summary>
		/// <typeparam name="TManager"></typeparam>
		/// <returns></returns>
		public static TManager GetManager<TManager>() where TManager : IRecordManager {
			if (!_managers.ContainsKey(typeof(TManager))) {
				lock (_managers) {
					if (!_managers.ContainsKey(typeof(TManager))) {
						TManager manager = Activator.CreateInstance<TManager>();
						_managers[typeof(TManager)] = manager;
					}
				}
			}

			return (TManager)_managers[typeof(TManager)];
		}

		/// <summary>
		/// Allows population of a specific RecordManager instance into the cache.
		/// </summary>
		/// <typeparam name="TManager"></typeparam>
		/// <param name="instance"></param>
		public static void SetManager<TManager>(IRecordManager instance) {
			lock (_managers) {
				_managers[typeof(TManager)] = instance;
			}
		}
	}
}
