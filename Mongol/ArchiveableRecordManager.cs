using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Mongol {
	public class ArchiveableRecordManager<T> : RecordManager<T> where T : Record {
		private static readonly log4net.ILog logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		protected internal virtual string ArchiveCollectionName {
			get {
				return "Archived_" + typeof(T).Name;
			}
		}

		/// <summary>
		/// Saves the item into a collection prefixed with "Archived_" and removes the document from the original collection.
		/// </summary>
		/// <param name="record"></param>
		public virtual void Archive(T record) {
			if (record == null) {
				logger.Error("Attempted to archive a null record.");
				throw new ArgumentNullException("record", "Cannot archive a null record.");
			}
			else {
				logger.DebugFormat("Archive({0}), ArchiveCollectionName=", record.Id, ArchiveCollectionName);
				collection.Database.GetCollection<T>(ArchiveCollectionName).Save(record);
				deleteById(record.Id);
			}
		}

		/// <summary>
		/// Archives multiple records.  Saves the items into a collection prefixed with "Archived_" and removes them from the original collection.
		/// </summary>
		/// <param name="records"></param>
		public virtual void Archive(IEnumerable<T> records) {
			if (records == null) {
				logger.Error("Attempted to call Archive on null collection.");
				throw new ArgumentNullException("records", "Cannot Archive a null collection");
			}
			else {
				foreach (T item in records) {
					Archive(item);
				}
			}
		}
	}
}
