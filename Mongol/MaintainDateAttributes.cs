using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mongol {
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
	public class MaintainModifiedDateAttribute : Attribute {
	}

	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
	public class MaintainCreatedDateAttribute : Attribute {
	}
}
