using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mongol {
	interface ITimeStampedRecord {
		DateTime CreatedDate { get; set; }
		DateTime ModifiedDate { get; set; }
	}
}
