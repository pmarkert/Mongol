using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using MongoDB.Bson;
using MongoDB.Driver.Builders;
using MongoDB.Driver;
using FluentMongo.Linq;
using MongoDB.Bson.Serialization;

namespace Mongol {
	/// <summary>
	/// The untyped RecordManager manager class is used to collect any constants, or statics that may be useful without needing to have a particular typed instance.
	/// </summary>
	public abstract class RecordManager {
		/// <summary>
		/// The field name MongoDB uses for the _id field.
		/// </summary>
		public const string ID_FIELD = "_id";
	}

	/// <summary>
	/// Repository gateway to a single collection named after the type <typeparamref name="T"/>
	/// </summary>
	/// <typeparam name="T">The type of object to be stored in the collection</typeparam>
	public abstract class RecordManager<T> : RecordManager, IRecordManager<T> where T : class {
		private static readonly log4net.ILog logger = log4net.LogManager.GetLogger(String.Format("{0}.RecordManager<{1}>", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.Namespace, typeof(T).Name));

		/// <summary>
		/// The underlying MongoDB collection that the RecordManager encapsulates
		/// </summary>
		protected virtual MongoCollection<T> collection {
			get { return Connection.Instance.GetCollection<T>(); }
		}

		/// <summary>
		/// Used to track when Initialize needs to be run (once per application run)
		/// </summary>
		private static bool Initialized = false;

		/// <summary>
		/// Creates a new RecordManager
		/// </summary>
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
		/// The Linq Queryable for the collection
		/// </summary>
		public virtual IQueryable<T> All {
			get {
				logger.Debug("All");
				return collection.AsQueryable();
			}
		}

		/// <summary>
		/// Deletes a single record from the collection by id.
		/// </summary>
		public virtual void DeleteById(object id) {
			logger.DebugFormat("Delete({0})", id);
			deleteById(id);
		}

		/// <summary>
		/// Retrieves a single record from the collection by Id
		/// </summary>
		public virtual T GetById(object id) {
			logger.DebugFormat("GetById({0})", id);
			if (id == null) {
				logger.Error("GetById() called with null id.");
				throw new ArgumentNullException("id");
			}
			return collection.FindOneById(BsonValue.Create(id));
		}

		/// <summary>
		/// Retrieves matching records by the ids.
		/// </summary>
		/// <remarks>Useful to solve 1-N problems on psuedo-joins. The list of ids should not contain an unreasonable number of items (Mongo has a limit of 4MB per query document).</remarks>
		/// <param name="ids">The list of Id's for which to find records.</param>
		public virtual IEnumerable<T> GetManyByIds(IEnumerable<object> ids) {
			logger.Debug("GetManyByIds(ids)");
			if (ids == null) {
				logger.Error("GetById() called with null value for ids enumeration.");
				throw new ArgumentNullException("ids");
			}
			return collection.Find(Query.In(ID_FIELD, new BsonArray(ids)));
		}

		/// <summary>
		/// Inserts many items into the collection in a single batch.
		/// </summary>
		/// <remarks>The number of items cannot be too large because there is a size-limit on messages, but it's pretty reasonable.</remarks>
		/// <returns>The number of items that were Inserted.</returns>
		public virtual void BatchInsert(IEnumerable<T> records) {
			logger.Debug("InsertMany(records)");
			if (records == null) {
				logger.Error("BatchInsert() called with null value for records enumeration.");
				throw new ArgumentNullException("records");
			}
			IEnumerable<SafeModeResult> result = null;
			if (records == null) {
				logger.Warn("Attempted to InsertMany on null (not empty, but null) collection. Nothing done.");
			}
			else {
				logger.Info("InsertMany called for " + records.Count() + " records.");
				List<T> materializedItems = records.Where(r => r != null).ToList();
				foreach (T record in materializedItems) {
					OnBeforeSave(record);
				}
				if (materializedItems.Count > 0) {
					result = collection.InsertBatch(materializedItems);
				}
				foreach (T record in materializedItems) {
					OnAfterSave(record);
				}
			}
		}

		/// <summary>
		/// Saves a record to the collection (either as an insert or update).  Replaces the entire record if updated.
		/// </summary>
		/// <returns>true if the record was inserted, false if it was overwritten.</returns>
		public virtual bool Save(T record) {
			if (record == null) {
				logger.Error("Cannot Save a null record.");
				throw new ArgumentNullException("record", "Cannot save a null record.");
			}
			else {
				logger.DebugFormat("Save({0})", record);
				OnBeforeSave(record);
				var safeModeResult = collection.Save(record);
				OnAfterSave(record);
				return !safeModeResult.UpdatedExisting;
			}
		}

		/// <summary>
		/// Override this method to do any once-per application run initialization (such as ensuring indexes), setting special ClassMap conventions, collection cleanup, etc.
		/// </summary>
		protected internal virtual void Initialize() {
			logger.Debug("Initialize()");
		}

		/// <summary>
		/// Retrieves the Id of a Record
		/// </summary>
		protected internal object GetRecordId(T record) {
			return BsonClassMap.LookupClassMap(typeof(T)).IdMemberMap.Getter(record);
		}

		/// <summary>
		/// Gets the number of items in the collection that match the optional query criteria (or all items if no query specified).
		/// </summary>
		/// <param name="criteria">An optional query to filter the counted items.</param>
		/// <returns>The number of items in the collection matching the filter.</returns>
		protected internal long Count(IMongoQuery criteria = null) {
			logger.DebugFormat("Count({0})", criteria);
			return collection.Count(criteria);
		}

		/// <summary>
		/// Executes a search query against the collection, optionally applying a sort, skip, and limit.
		/// </summary>
		/// <remarks>Use common sense about indexing common queries, especially for sizeable collections.</remarks>
		protected internal virtual IEnumerable<T> Find(IMongoQuery criteria, IMongoSortBy sort = null, int? skip = null, int? limit = null) {
			logger.DebugFormat("Find({0},{1},{2},{3})", criteria, sort, skip, limit);
			return find(criteria, sort, skip, limit);
		}

		/// <summary>
		/// Finds the one and only item in the collection matching the criteria.  Throws an exception if multiple items were matched.
		/// </summary>
		/// <returns>The item or null if no matches are found.</returns>
		protected internal virtual T FindSingle(IMongoQuery criteria) {
			logger.DebugFormat("FindSingle({0})", criteria);
			return find(criteria).SingleOrDefault();
		}

		/// <summary>
		/// Event hook called becore a record is saved (either single or in batch).
		/// </summary>
		protected internal virtual void OnBeforeSave(T record) {
			if (record is ITimeStampedRecord) {
				ITimeStampedRecord tsRecord = record as ITimeStampedRecord;
				var now = DateTime.UtcNow;
				if (tsRecord.CreatedDate == DateTime.MinValue) {
					tsRecord.CreatedDate = now;
				}
				tsRecord.ModifiedDate = now;
			}
		}

		/// <summary>
		/// Event hook called after a record is saved.
		/// </summary>
		/// <param name="record"></param>
		protected internal virtual void OnAfterSave(T record) {
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
		/// Atomically finds and modifies at most one item in the collection, returning the item.
		/// </summary>
		/// <param name="update">The update command to modify the item.  The update operation should usually cause the item to no longer match the query criteria.</param>
		/// <param name="sortBy">The sort order to use (the first matching item is used)</param>
		/// <param name="returnModifiedVersion">If true, returns the post-modification version of the item, otherwise the item as it was before modification.  Default=true</param>
		/// <remarks>Useful for managing concurrency across multiple processes such as a Queue.  Can also be used to setup items that go through a workflow.</remarks>
		protected internal virtual T FindOneAndModify(IMongoQuery criteria, IMongoUpdate update, IMongoSortBy sortBy = null, bool returnModifiedVersion = true) {
			logger.DebugFormat("FindAndModify({0},{1},{2},{3})", criteria, update, sortBy, returnModifiedVersion);
			return collection.FindAndModify(criteria, sortBy, update, returnModifiedVersion).GetModifiedDocumentAs<T>();
		}

		/// <summary>
		/// Automically finds and removes at most one item from the collection, returning the item.
		/// </summary>
		/// <param name="orderBy">The sort order to use (the first matching item is used)</param>
		protected internal virtual T FindOneAndRemove(IMongoQuery criteria, IMongoSortBy sortBy = null) {
			logger.DebugFormat("FindOneAndRemove({0},{1})", criteria, sortBy);
			var removeResult = collection.FindAndRemove(criteria, sortBy);
			if (removeResult.ModifiedDocument != null) {
				return removeResult.GetModifiedDocumentAs<T>();
			}
			else {
				return null;
			}
		}

		/// <summary>
		/// Enumerates FindAndModify, be certain that your update causes documents to no longer match the query, or you will end up with an infinite loop.  
		/// This is thread-safe to work across multiple callers (and processes) since it uses Mongo to atomically find and modify.
		/// </summary>
		/// <param name="criteria">The criteria for which to find items</param>
		/// <param name="update">The update command to modify the items.</param>
		/// <param name="returnModifiedVersion">If true, returns the post-modification version of the item, otherwise the item as it was before modification. Default=true</param>
		protected internal IEnumerable<T> EnumerateAndModify(IMongoQuery criteria, IMongoUpdate update, IMongoSortBy sortBy = null, bool returnModifiedVersion = true) {
			logger.DebugFormat("EnumerateAndModify({0},{1},{2},{3})", criteria, update, sortBy, returnModifiedVersion);
			T result;
			while ((result = FindOneAndModify(criteria, update, sortBy, returnModifiedVersion)) != null) {
				yield return result;
			}
		}

		/// <summary>
		/// Enumerates FindAndRemove, removing each one from the database as it is returned. 
		/// This is thread-safe to work across multiple callers (and processes) since it uses Mongo to atomically find and remove.
		/// </summary>
		/// <param name="criteria">The criteria for which to find items</param>
		protected internal IEnumerable<T> EnumerateAndRemove(IMongoQuery criteria, IMongoSortBy sortBy = null) {
			logger.DebugFormat("EnumerateAndRemove({0},{1})", criteria, sortBy);
			T result;
			while ((result = FindOneAndRemove(criteria, sortBy)) != null) {
				yield return result;
			}
		}

		/// <summary>
		/// Updates many documents at once.
		/// </summary>
		/// <param name="update">The update operation to apply to the matched items.</param>
		/// <param name="asUpsert">If true, MongoDB will attempt to create new items based upon the query values passed in.</param>
		/// <returns>The number of items updated.</returns>
		protected internal virtual long UpdateMany(IMongoQuery criteria, UpdateBuilder update, bool asUpsert = false) {
			logger.DebugFormat("UpdateMany({0},{1},{2})", criteria, update, asUpsert);
			if (typeof(ITimeStampedRecord).IsAssignableFrom(typeof(T))) {
				update = update.Combine(Update.Set(PropertyNameResolver<ITimeStampedRecord>.Resolve(x => x.ModifiedDate), DateTime.UtcNow));
			}
			return collection.Update(criteria, update, asUpsert ? UpdateFlags.Multi | UpdateFlags.Upsert : UpdateFlags.Multi).DocumentsAffected;
		}

		/// <summary>
		/// Deletes multiple items in the colleciton by query criteria.
		/// </summary>
		/// <returns>The number of documents deleted.</returns>
		protected internal virtual long DeleteMany(IMongoQuery criteria) {
			logger.DebugFormat("DeleteMany({0})", criteria);
			if (criteria == null) {
				logger.Error("Attempted to call DeleteMany with a null query.");
				throw new ArgumentNullException("query", "Cannot call DeleteMany with a null query");
			}
			return collection.Remove(criteria).DocumentsAffected;
		}

		/// <summary>
		/// Returns a string representation of the specified expression.  This allows you to strongly type your query criteria, so they can be passed back to Mongo as strings.
		/// </summary>
		/// <param name="expression">A member expression on the record type.</param>
		/// <returns>A string representing the member expression</returns>
		protected internal static string PropertyName<S>(Expression<Func<T, S>> expression) {
			// TODO - Add caching here if the lambdas get to be too expensive?  But the cache may not help if the building of the expression is actually the expensive part
			MemberExpression expressionBody = expression.Body as MemberExpression;
			if (expressionBody == null) {
				throw new ApplicationException("Expression must be member access.");
			}
			return PropertyNameResolver<T>.Resolve(expression);
		}

		/// <summary>
		/// Ensures that the specified index is created on the collection.
		/// </summary>
		/// <remarks>Can be called multiple times in-expensively.</remarks>
		protected internal virtual void EnsureIndex(IMongoIndexKeys keys, IMongoIndexOptions options) {
			logger.DebugFormat("EnsureIndex({0},{1})", keys, options);
			collection.EnsureIndex(keys, options);
		}

		/// <summary>
		/// Convenience method to create a Query criteria based upon the ID field.
		/// </summary>
		/// <param name="Id">The value of the Id for which to query</param>
		/// <returns>A MongoQuery criteria for the equality of the specified Id.</returns>
		protected internal static IMongoQuery QueryCriteria_ById(object Id) {
			if (Id == null) {
				logger.Error("Attempted to call QueryCriteriaById with a null Id");
				throw new ArgumentNullException("Id", "Cannot QueryCriteriaById with a null Id");
			}
			return Query.EQ(ID_FIELD, BsonValue.Create(Id));
		}

		/// <summary>
		/// Convenience method to create a Query criteria based upon the ID field.
		/// </summary>
		/// <param name="Id">A list of values for Ids for which to query</param>
		/// <returns>A MongoQuery criteria for the equality of the specified Id.</returns>
		protected internal static IMongoQuery QueryCriteria_ById(IEnumerable<object> IdList) {
			if (IdList == null) {
				logger.Error("Attempted to call QueryCriteriaById with a null Id");
				throw new ArgumentNullException("Id", "Cannot QueryCriteriaById with a null Id");
			}
			return Query.In(ID_FIELD, BsonArray.Create(IdList));
		}

		protected internal IEnumerable<T> find(IMongoQuery criteria, IMongoSortBy sortBy = null, int? skip = null, int? limit = null) {
			MongoCursor<T> cursor = collection.Find(criteria);
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
		/// 
		/// </summary>
		/// <param name="id"></param>
		/// <returns></returns>
		protected internal SafeModeResult deleteById(object id) {
			if (id == null) {
				logger.Error("Delete() called without specifying an Id");
				throw new ArgumentNullException("Id must be specified for deletion.");
			}
			else {
				return collection.Remove(QueryCriteria_ById(id));
			}
		}
	}
}
