using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;

namespace Mongol {
	/// <summary>
	/// Marks that a record should have creation and modification date/time maintained by the RecordManager
	/// </summary>
	public interface IAuditedRecord {
		/// <summary>
		/// The date/time the record was first created.
		/// </summary>
		DateTime CreatedDate { get; set; }

		/// <summary>
		/// The date/time the record was last updated.
		/// </summary>
		DateTime ModifiedDate { get; set; }
	}
}
