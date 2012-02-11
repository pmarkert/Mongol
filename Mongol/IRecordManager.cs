using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver.Builders;
using MongoDB.Driver;

namespace Mongol {
	public interface IRecordManager {
	}

	public interface IRecordManager<T> : IRecordManager {
		IQueryable<T> All { get; }
		void DeleteById(object id);
		T GetById(object id);
		IEnumerable<T> GetManyByIds(IEnumerable<object> ids);
		bool Save(T record);
		void BatchInsert(IEnumerable<T> records);
	}
}
