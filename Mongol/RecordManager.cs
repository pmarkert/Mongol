using System.Collections.Generic;
using System.Linq;
using log4net;
using System;
using MongoDB.Bson;
using MongoDB.Driver.Builders;
using MongoDB.Driver;
using System.Linq.Expressions;

namespace Mongol {
	public abstract class RecordManager {
		protected internal const string ID_FIELD = "_id";
	}

	/// <summary>
	/// A RecordManager serves as a repository gateway to a single collection named after the type <typeparamref name="T"/>
	/// </summary>
	/// <typeparam name="T">The type of object to be stored in the collection</typeparam>
	public abstract class RecordManager<T> : RecordManager, IRecordManager<T> where T : Record {
		private static readonly ILog logger = LogManager.GetLogger(String.Format("{0}.RecordManager<{1}>", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.Namespace, typeof(T).Name));

		private static bool Initialized = false;

		/// <summary>
		/// The underlying MongoDB collection that the RecordManager encapsulates
		/// </summary>
		protected virtual MongoCollection<T> collection {
			get { return Connection.Instance.GetCollection<T>(); }
		}

		protected RecordManager() {
			if (!Initialized) {
				lock (typeof(RecordManager<T>)) {
					if (!Initialized) {
						Initialize();
					}
				}
			}
		}

		/// <summary>
		/// Override this method to do any once-per application instance initialization (such as ensuring indexes)
		/// </summary>
		protected virtual void Initialize() {
			logger.Debug("Initialize()");
		}

		/// <summary>
		/// Enumeration of the entire collection (Danger Will Robinson!)  Use the force wisely.
		/// </summary>
		public virtual IEnumerable<T> All {
			get {
				logger.Debug("All");
				return collection.FindAll();
			}
		}

		/// <summary>
		/// Returns the number of items in the collection that match the optional query criteria (or all items if not specified).
		/// </summary>
		/// <param name="query">An optional query to filter the counted items.</param>
		/// <returns>The number of items in the collection matching the filter.</returns>
		public long Count(IMongoQuery query = null) {
			logger.DebugFormat("Count({0})", query);
			return collection.Count(query);
		}

		/// <summary>
		/// Retrieves a single record by Id
		/// </summary>
		/// <param name="Id">The Id of the record to be retrieved.</param>
		/// <returns>The specified record or null if not found</returns>
		public virtual T GetById(string Id) {
			logger.DebugFormat("GetById({0})", Id);
			if (Id == null) {
				logger.Error("GetById() called without an Id.");
				throw new ArgumentNullException("id", "Cannot GetById with a null Id");
			}
			return collection.FindOneById(Id);
		}

		/// <summary>
		/// Returns an enumerable list of items by the ids.  Useful to solve 1-N problems on psuedo-joins. The list of Ids must have a reasonable number.
		/// </summary>
		/// <param name="ids">The list of Id's for which to find records.  Keep it sensible.</param>
		/// <returns>Items from the collection matching the ids.</returns>
		public virtual IEnumerable<T> GetManyByIds(IEnumerable<string> ids) {
			logger.Debug("GetManyByIds(ids)");
			return collection.Find(Query.In(ID_FIELD, new BsonArray(ids)));
		}

		/// <summary>
		/// Executes a search query against the collection, optionally applying a sort, skip, and limit. Normal common sense about indexing is worth keeping in mind for sizeable collections.
		/// </summary>
		/// <param name="query">The query to execute.</param>
		/// <param name="sortBy">Optionally, the order in which to return items</param>
		/// <param name="skip">Optionally, the number of items to skip from the beginning of the list</param>
		/// <param name="limit">Optionally, the maximum number of items to be returned</param>
		/// <returns>An enumerable list of items matching the criteria</returns>
		protected internal virtual IEnumerable<T> Find(IMongoQuery query, IMongoSortBy sortBy = null, int? skip = null, int? limit = null) {
			logger.DebugFormat("Find({0},{1},{2},{3})", query, sortBy, skip, limit);
			return find(query, sortBy, skip, limit);
		}

		private IEnumerable<T> find(IMongoQuery query, IMongoSortBy sortBy = null, int? skip = null, int? limit = null) {
			MongoCursor<T> cursor = collection.Find(query);
			if (sortBy != null) {
				cursor.SetSortOrder(sortBy);
			}
			if (skip.HasValue) {
				cursor.SetSkip(skip.Value);
			}
			if (limit.HasValue) {
				cursor.SetLimit(limit.Value);
			}
			return cursor;
		}

		/// <summary>
		/// Finds the one and only item in the collection matching the criteria.  Throw an exception if multiple items are matched.
		/// </summary>
		/// <param name="query">The query to filter the items</param>
		/// <returns>The item or null if no matches are found.</returns>
		protected internal virtual T FindSingle(IMongoQuery query) {
			logger.DebugFormat("FindSingle({0})", query);
			return find(query).SingleOrDefault();
		}

		private void delete(string id) {
			if (id == null) {
				logger.Error("Delete() called without specifying an Id");
				throw new ArgumentNullException("Id must be specified for deletion.");
			}
			else {
				collection.Remove(QueryCriteria_ById(id));
			}
		}

		/// <summary>
		/// Deletes a single record from the collection
		/// </summary>
		/// <param name="record">The id of the record to be deleted.</param>
		public virtual void Delete(string id) {
			logger.DebugFormat("Delete({0})", id);
			delete(id);
		}

		/// <summary>
		/// Inserts many items into the collection in a batch.  The number of items cannot be too large because there is a size-limit on messages, but it's pretty reasonable.  If records are Audited, they will
		/// be properly time-stamped.
		/// </summary>
		/// <param name="records">Enumerable list of records to be inserted</param>
		public virtual void InsertMany(IEnumerable<T> records) {
			if (records == null) {
				logger.Warn("Attempted to InsertMany on null (not empty, but null) collection. Nothing done.");
			}
			else {
				logger.Info("InsertMany called for " + records.Count() + " records.");
				List<T> materializedItems = records.ToList();
				foreach (T record in materializedItems) {
					OnBeforeSave(record);
				}
				if (materializedItems.Count > 0) {
					collection.InsertBatch(materializedItems);
				}
				foreach (T record in materializedItems) {
					OnAfterSave(record);
				}
			}
		}

		/// <summary>
		/// Called becore a record is saved (single or in batch).  Sets the Id if null and updates Audit time-stamps as needed.
		/// </summary>
		/// <param name="record">The that is about to be saved.</param>
		protected virtual void OnBeforeSave(T record) {
			if (record == null) {
				logger.Error("OnBeforeSave called with a null record.");
				throw new ArgumentNullException("Cannot call OnBeforeSave with a null record.");
			}
			logger.DebugFormat("OnBeforeSave({0})", record.Id);
			if (record.Id == null) {
				record.Id = ObjectId.GenerateNewId().ToString();
				logger.DebugFormat("Generated new Id for record - " + record.Id);
			}
			IAuditedRecord audited = record as IAuditedRecord;
			if (audited != null) {
				var now = DateTime.UtcNow;
				logger.DebugFormat("AuditDate for record - " + now);
				if (audited.CreatedDate == DateTime.MinValue) {
					audited.CreatedDate = now;
				}
				audited.ModifiedDate = now;
			}
		}

		/// <summary>
		/// Called after a record is saved.
		/// </summary>
		/// <param name="record"></param>
		protected virtual void OnAfterSave(T record) {
			logger.DebugFormat("OnAfterSave({0})", record.Id);
		}

		/// <summary>
		/// Saves a record to the database (either new or updated).  Replaces the entire record if updated.
		/// </summary>
		/// <param name="record">The record to be inserted or updated.</param>
		public virtual void Save(T record) {
			if (record == null) {
				logger.Error("Cannot Save a null record.");
				throw new ArgumentNullException("record", "Cannot save a null record.");
			}
			else {
				OnBeforeSave(record);
				collection.Save(record);
				OnAfterSave(record);
			}
		}

		/// <summary>
		/// Removes the entire collection from the database (dropping indexes too).
		/// </summary>
		protected internal virtual void DropCollection() {
			logger.Warn("DropCollection");
			if (collection.Exists()) {
				collection.Drop();
			}
		}

		/// <summary>
		/// Automically finds and modifies at most one item, returning the item.
		/// </summary>
		/// <param name="query">The criteria for which to find an item.</param>
		/// <param name="update">The update command to modify the item</param>
		/// <param name="orderBy">The sort order to use (first matching item is used)</param>
		/// <param name="returnModifiedVersion">If true, returns the post-modification version of the item, otherwise the item as it was before modification.  Default=true</param>
		/// <returns></returns>
		protected internal virtual T FindOneAndModify(IMongoQuery query, IMongoUpdate update, IMongoSortBy sortBy = null, bool returnModifiedVersion = true) {
			logger.DebugFormat("FindAndModify({0},{1},{2},{3})", query, update, sortBy, returnModifiedVersion);
			return collection.FindAndModify(query, sortBy, update, returnModifiedVersion).GetModifiedDocumentAs<T>();
		}

		protected internal virtual T FindOneAndRemove(IMongoQuery query, IMongoSortBy sortBy = null) {
			logger.DebugFormat("FindOneAndRemove({0},{1})", query, sortBy);
			var removeResult = collection.FindAndRemove(query, sortBy);
			if (removeResult.ModifiedDocument != null) {
				return removeResult.GetModifiedDocumentAs<T>();
			}
			else {
				return null;
			}
		}

		/// <summary>
		/// Executes FindAndModify in an enumerable list of items, be certain that your update causes documents to no longer match the query, or you will end up with an infinite loop.  This is thread-safe
		/// to work across multiple callers (and processes) since it uses the database to atomically find and modify.
		/// </summary>
		/// <param name="query">The criteria for which to find items</param>
		/// <param name="update">The update command to modify the items.</param>
		/// <param name="sortBy"></param>
		/// <param name="returnModifiedVersion">If true, returns the post-modification version of the item, otherwise the item as it was before modification. Default=true</param>
		/// <returns></returns>
		protected internal IEnumerable<T> EnumerateAndModify(IMongoQuery query, IMongoUpdate update, IMongoSortBy sortBy = null, bool returnModifiedVersion = true) {
			logger.DebugFormat("EnumerateAndModify({0},{1},{2},{3})", query, update, sortBy, returnModifiedVersion);
			T result;
			do {
				result = FindOneAndModify(query, update, sortBy, returnModifiedVersion);
				if (result != null) {
					yield return result;
				}
			} while (result != null);
		}

		/// <summary>
		/// Executes FindAndRemove to produce an enumerable list of maching items, removing each one from the database as it is returned. This is thread-safe
		/// to work across multiple callers (and processes) since it uses the database to atomically find and remove.
		/// </summary>
		/// <param name="query">The criteria for which to find items</param>
		/// <param name="sortBy"></param>
		protected internal IEnumerable<T> EnumerateAndRemove(IMongoQuery query, IMongoSortBy sortBy = null) {
			logger.DebugFormat("EnumerateAndRemove({0},{1})", query, sortBy);
			T result;
			do {
				var removeResult = collection.FindAndRemove(query, sortBy);
				if (removeResult.ModifiedDocument != null) {
					result = removeResult.GetModifiedDocumentAs<T>();
					yield return result;
				}
				else {
					result = null;
				}
			} while (result != null);
		}

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
				delete(record.Id);
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

		/// <summary>
		/// Convenience method to create a Query criteria based upon the ID field.
		/// </summary>
		/// <param name="Id">The value of the Id for which to query</param>
		/// <returns>A MongoQuery criteria for the equality of the specified Id.</returns>
		protected static IMongoQuery QueryCriteria_ById(string Id) {
			if (Id == null) {
				logger.Error("Attempted to call QueryCriteriaById with a null Id");
				throw new ArgumentNullException("Id", "Cannot QueryCriteriaById with a null Id");
			}
			return Query.EQ(ID_FIELD, Id);
		}

		/// <summary>
		/// Convenience method to create a Query criteria based upon the ID field.
		/// </summary>
		/// <param name="Id">The value of the Id for which to query</param>
		/// <returns>A MongoQuery criteria for the equality of the specified Id.</returns>
		protected static IMongoQuery QueryCriteria_ById(IEnumerable<string> IdList) {
			if (IdList == null) {
				logger.Error("Attempted to call QueryCriteriaById with a null Id");
				throw new ArgumentNullException("Id", "Cannot QueryCriteriaById with a null Id");
			}
			return Query.In(ID_FIELD, BsonArray.Create(IdList));
		}

		/// <summary>
		/// Updates many documents at once. If the item is an AuditedRecord, DateModified will be properly timestamped.
		/// </summary>
		/// <param name="query">The criteria for which to match items.</param>
		/// <param name="update">The update operation to apply to the matched items.</param>
		/// <param name="asUpsert">If true, MongoDB will attempt to create new items based upon the query values passed in.</param>
		/// <returns>SafeModeResult with count of items</returns>
		public virtual SafeModeResult UpdateMany(IMongoQuery query, UpdateBuilder update, bool asUpsert = false) {
			logger.DebugFormat("UpdateMany({0},{1},{2})", query, update, asUpsert);
			if (typeof(T).IsAssignableFrom(typeof(IAuditedRecord))) {
				var auditDate = DateTime.UtcNow;
				logger.DebugFormat("Records are auditable, so adding criteria to update to set AuditDate to - " + auditDate);
				update = Update.Set("ModifiedDate", auditDate).Combine(update);
			}
			var updateFlags = UpdateFlags.Multi;
			if (asUpsert) {
				updateFlags = updateFlags | UpdateFlags.Upsert;
			}
			return collection.Update(query, update, updateFlags);
		}

		/// <summary>
		/// Deletes multiple items in the colleciton by query criteria.
		/// </summary>
		/// <param name="query">The critier for which to match items for deletion.</param>
		/// <returns>SafeModeResult with count of items.</returns>
		public virtual SafeModeResult DeleteMany(IMongoQuery query) {
			logger.DebugFormat("DeleteMany({0})", query);
			if (query == null) {
				logger.Error("Attempted to call DeleteMany with a null query.");
				throw new ArgumentNullException("query", "Cannot call DeleteMany with a null query");
			}
			return collection.Remove(query);
		}

		/// <summary>
		/// Returns a string representation of the specified expression.  This allows you to strongly type your query criteria, so they can be passed back to Mongo as strings.
		/// </summary>
		/// <param name="expression">A member expression on the record type.</param>
		/// <returns>A string representing the member expression</returns>
		protected internal static string PropertyName<S>(Expression<Func<T, S>> expression) {
			// TODO - Add caching here if it gets to be too expensive?  But the cache may not help if the building of the expression is actually the expensive part
			MemberExpression expressionBody = expression.Body as MemberExpression;
			if (expressionBody == null) {
				throw new ApplicationException("Expression must be member access.");
			}
			return PropertyNameResolver<T>.Resolve(expression);
		}

		/// <summary>
		/// Ensures that the specified index is created on the collection.  Can be called multiple times in-expensively.
		/// </summary>
		/// <param name="keys"></param>
		/// <param name="options"></param>
		protected internal virtual void EnsureIndex(IMongoIndexKeys keys, IMongoIndexOptions options) {
			logger.DebugFormat("EnsureIndex({0},{1})", keys, options);
			collection.EnsureIndex(keys, options);
		}
	}
}
