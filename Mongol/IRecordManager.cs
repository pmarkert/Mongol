using System;
using MongoDB.Driver.Builders;
using MongoDB.Driver;
using System.Collections.Generic;
namespace Mongol {
	public interface IRecordManager {
	}

	public interface IRecordManager<T> : IRecordManager {
		IEnumerable<T> All { get; }
		void Archive(IEnumerable<T> records);
		void Archive(T record);
		SafeModeResult Delete(string id);
		SafeModeResult DeleteMany(IMongoQuery query);
		T GetById(string id);
		IEnumerable<T> GetManyByIds(IEnumerable<string> ids);
		void Save(T record);
		IEnumerable<SafeModeResult> InsertMany(IEnumerable<T> records);
		MongoDB.Driver.SafeModeResult UpdateMany(IMongoQuery query, UpdateBuilder update, bool asUpsert = false);
	}
}
