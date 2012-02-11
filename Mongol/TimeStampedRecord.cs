using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mongol {
	public class TimeStampedRecord : Record, ITimeStampedRecord {
		public DateTime CreatedDate { get; set; }
		public DateTime ModifiedDate { get; set; }
	}
}
