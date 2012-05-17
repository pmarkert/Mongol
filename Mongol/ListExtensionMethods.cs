using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mongol {
	/// <summary>
	/// List extensions for Mongol
	/// </summary>
	public static class ListExtensions {
		/// <summary>
		/// Provides a mechanism to dereference child elements of a collection, removing the prefix of the parent.  Used for Lambda expressions that need to access properties of child elements.
		/// </summary>
		public static T Relative<T>(this IEnumerable<T> list) {
			throw new NotImplementedException("This method is not intended to be invoked, only for PropertyName specification");
		}

		/// <summary>
		/// Provides a mechanism to dereference child elements of a collection.  Used for Lambda expressions that need to access properties of child elements.
		/// </summary>
		public static T Member<T>(this IEnumerable<T> list) {
			throw new NotImplementedException("This method is not intended to be invoked, only for PropertyName specification");
		}
	}
}
