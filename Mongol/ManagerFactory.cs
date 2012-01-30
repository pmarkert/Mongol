using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mongol {
	public class ManagerFactory {

		private static Dictionary<Type, IRecordManager> _managers = new Dictionary<Type, IRecordManager>();

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
	}
}
