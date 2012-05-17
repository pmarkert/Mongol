/* Copyright 2012 Ephisys Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
   limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver.Builders;
using MongoDB.Driver;

namespace Mongol {
	/// <summary>
	/// Marker interface to identify instances of RecordManager as a polymorphic base handle.
	/// </summary>
	public interface IRecordManager {
	}

	/// <summary>
	/// Interface implemented by RecordManager instances containing the basic publicly exposed CRUD operations for the repository.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public interface IRecordManager<T> : IRecordManager {
		/// <summary>
		/// The Linq Queryable for the collection
		/// </summary>
		IQueryable<T> AsQueryable { get; }

		/// <summary>
		/// Deletes a single record from the collection by id.
		/// </summary>
		void DeleteById(object id);

		/// <summary>
		/// Retrieves a single record from the collection by Id
		/// </summary>
		T GetById(object id);

		/// <summary>
		/// Retrieves matching records by the ids.
		/// </summary>
		/// <remarks>Useful to solve 1-N problems on psuedo-joins. The list of ids should not contain an unreasonable number of items (Mongo has a limit of 4MB per query document).</remarks>
		/// <param name="ids">The list of Id's for which to find records.</param>
		IEnumerable<T> GetManyByIds(IEnumerable<object> ids);

		/// <summary>
		/// Saves a record to the collection (either as an insert or update).  Replaces the entire record if updated.
		/// </summary>
		/// <returns>true if the record was inserted, false if it was overwritten or if the connection is not using SafeMode.</returns>
		bool Save(T record);

		/// <summary>
		/// Inserts many items into the collection in a single batch.
		/// </summary>
		/// <remarks>The number of items cannot be too large because there is a size-limit on messages, but it's pretty reasonable.</remarks>
		/// <returns>The number of items that were Inserted.</returns>
		void BatchInsert(IEnumerable<T> records);
	}
}
