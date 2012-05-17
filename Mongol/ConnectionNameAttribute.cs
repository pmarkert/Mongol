using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mongol {
	/// <summary>
	/// Used to override the name of the Mongol Connection used by the RecordManager.
	/// </summary>
	[AttributeUsage(AttributeTargets.Class)]
	public class ConnectionNameAttribute : Attribute {
		/// <summary>
		/// The name of the Mongol Connection to be used.
		/// </summary>
		public string ConnectionName { get; set; }

		/// <summary>
		/// Initializes a new instance of the attribute, specifying the connection name.
		/// </summary>
		/// <param name="ConnectionName"></param>
		public ConnectionNameAttribute(string ConnectionName) {
			this.ConnectionName = ConnectionName;
		}
	}
}
