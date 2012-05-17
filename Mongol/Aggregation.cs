using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;
using Mongol;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Mongol {
	/// <summary>
	/// Static convenience methods for generating Aggregation Framework pipeline commands and arguments
	/// </summary>
	public static class Aggregation {
		/// <summary>
		/// Generates a $match pipeline command based upon the given query
		/// </summary>
		/// <param name="query">The query used to filter the pipeline document</param>
		public static BsonDocument Match(QueryComplete query) {
			return new BsonDocument("$match", query.ToBsonDocument());
		}

		/// <summary>
		/// Generates a $project pipeline command based upon the provided projection specifications
		/// </summary>
		/// <param name="projectionFields">A params array of projection specifications</param>
		public static BsonDocument Project(params BsonElement[] projectionFields) {
			return Project((IEnumerable<BsonElement>)projectionFields);
		}

		/// <summary>
		/// Generates a $project pipeline command based upon the provided projection specifications
		/// </summary>
		/// <param name="projectionFields">An enumerable of projection specifications</param>
		public static BsonDocument Project(IEnumerable<BsonElement> projectionFields) {
			return new BsonDocument("$project", new BsonDocument(projectionFields));
		}

		/// <summary>
		/// Static methods to generate projection specifications
		/// </summary>
		public static class Projection {
			/// <summary>
			/// Creates an inclusion projection to add a field to the projected document
			/// </summary>
			/// <param name="FieldName"></param>
			/// <returns></returns>
			public static BsonElement Include(string FieldName) {
				return new BsonElement(FieldName, 1);
			}

			/// <summary>
			/// Creates an exclusion projection to specifically remove a field from the projected document
			/// </summary>
			/// <param name="FieldName"></param>
			/// <returns></returns>
			public static BsonElement Exclude(string FieldName) {
				return new BsonElement(FieldName, 0);
			}

			/// <summary>
			/// Creates a mapping projection to rename or create a field in the projected document based upon the given expression
			/// </summary>
			/// <param name="Expression"></param>
			/// <param name="FieldName"></param>
			/// <returns></returns>
			public static BsonElement ProjectAs(string Expression, string FieldName) {
				return new BsonElement(FieldName, Expression);
			}
		}

		/// <summary>
		/// Generates a $group pipeline command based upon the specified group-by and grouping-aggregation specifications
		/// </summary>
		/// <param name="GroupBy">The group-by specification for grouping distinction</param>
		/// <param name="Aggregations">params array of grouping-aggregation expressions</param>
		public static BsonDocument Group(BsonElement GroupBy, params BsonElement[] Aggregations) {
			return Group(GroupBy, (IEnumerable<BsonElement>)Aggregations);
		}

		/// <summary>
		/// Generates a $group pipeline command based upon the specified group-by and grouping-aggregation specifications
		/// </summary>
		/// <param name="GroupBy">The group-by specification for grouping distinction</param>
		/// <param name="Aggregations">An enumerable of grouping-aggregation expressions</param>
		public static BsonDocument Group(BsonElement GroupBy, IEnumerable<BsonElement> Aggregations) {
			var value = new BsonDocument(GroupBy);
			if (Aggregations != null && Aggregations.Any()) {
				value.Add(Aggregations);
			}
			return new BsonDocument() { { "$group", value } };
		}


		/// <summary>
		/// Static methods to generate group-by and grouping-aggregation specifications
		/// </summary>
		public static class Grouping {
			/// <summary>
			/// Creates a group-by nothing specification where all items are grouped into a single bucket
			/// </summary>
			public static BsonElement ByNothing {
				get {
					return new BsonElement(RecordManager.ID_FIELD, BsonNull.Value);
				}
			}

			/// <summary>
			/// Creates a group-by specification on a single field
			/// </summary>
			/// <param name="Field">The name of the field to group by</param>
			public static BsonElement By(string Field) {
				return new BsonElement(RecordManager.ID_FIELD, "$" + Field);
			}

			/// <summary>
			/// Creates a group-by specification on multiple fields.  The grouping _id becomes an object with each field as a property.
			/// </summary>
			/// <param name="Fields">An enumerable list of fields</param>
			public static BsonElement By(IEnumerable<string> Fields) {
				if (Fields == null || !Fields.Any()) {
					return ByNothing;
				}
				var docFields = new BsonDocument();
				foreach (string field in Fields) {
					docFields.Add(field, 1);
				}
				return new BsonElement(RecordManager.ID_FIELD, docFields);
			}

			/// <summary>
			/// Creates a group-by specification on multiple fields.  The grouping _id becomes an object with each field as a property.
			/// </summary>
			/// <param name="Fields">A params array of fields</param>
			public static BsonElement By(params string[] Fields) {
				return By((IEnumerable<string>)Fields);
			}

			/// <summary>
			/// Creates a grouping-aggregation specification to $sum a field or expression
			/// </summary>
			/// <param name="FieldName">The field name for the grouping value in the output document</param>
			/// <param name="Expression">If specified, the expression to be summed.  Otherwise, the same field name from the input document will be used.</param>
			public static BsonElement Sum(string FieldName, string Expression = null) {
				return ApplyAggregateFunction(FieldName, Expression, "$sum");
			}

			/// <summary>
			/// Creates a grouping-aggregation specification to find the $first value of a field or expression
			/// </summary>
			/// <param name="FieldName">The field name for the grouping value in the output document</param>
			/// <param name="Expression">If specified, the expression to be examined.  Otherwise, the same field name from the input document will be used.</param>
			public static BsonElement First(string FieldName, string Expression = null) {
				return ApplyAggregateFunction(FieldName, Expression, "$first");
			}

			/// <summary>
			/// Creates a grouping-aggregation specification to find the $max value of a field or expression
			/// </summary>
			/// <param name="FieldName">The field name for the grouping value in the output document</param>
			/// <param name="Expression">If specified, the expression to be examined.  Otherwise, the same field name from the input document will be used.</param>
			public static BsonElement Max(string FieldName, string Expression = null) {
				return ApplyAggregateFunction(FieldName, Expression, "$max");
			}

			/// <summary>
			/// Creates a grouping-aggregation specification to add the value to an array if not already in the array.
			/// </summary>
			/// <param name="FieldName">The field name for the grouping value in the output document</param>
			/// <param name="Expression">If specified, the expression to be added to the set.  Otherwise, the same field name from the input document will be used.</param>
			public static BsonElement AddToSet(string FieldName, string Expression) {
				return ApplyAggregateFunction(FieldName, Expression, "$addToSet");
			}

			/// <summary>
			/// Creates a grouping-aggregation specification to count the number of items.
			/// </summary>
			/// <param name="FieldName">The field name for the grouping value in the output document</param>
			/// <returns></returns>
			public static BsonElement Count(string FieldName) {
				return ApplyAggregateFunction(FieldName, 1, "$sum");
			}

			private static object UseOrPrepareExpression(string FieldName, object Expression) {
				if (Expression == null) {
					return "$" + FieldName;
				}
				return Expression;
			}

			private static BsonElement ApplyAggregateFunction(string FieldName, object Expression, string aggregationFunction) {
				Expression = UseOrPrepareExpression(FieldName, Expression);
				return new BsonElement(FieldName, new BsonDocument(aggregationFunction, BsonValue.Create(Expression)));
			}
		}

		/// <summary>
		/// Generates a $sort pipeline command based upon the specified sort expression
		/// </summary>
		/// <param name="Sort">The sort expression</param>
		public static BsonDocument Sort(BsonDocument Sort) {
			return new BsonDocument("$sort", Sort);
		}

		/// <summary>
		/// Static methods to generate aggregation sort expressions
		/// </summary>
		public static class Sorting {
			/// <summary>
			/// Creates a sort expression by a single field
			/// </summary>
			/// <param name="field">The field or expression to sort by</param>
			/// <param name="ascending">True if ascending (default), false if descending</param>
			public static BsonDocument By(String field, bool ascending = true) {
				return new BsonDocument(field, ascending?1:-1);
			}

			/// <summary>
			/// Creates a sort expression by multiple fields
			/// </summary>
			/// <param name="fields">An enumerable list of fields or expressions to sort by</param>
			/// <param name="ascending">True if ascending (default), false if descending</param>
			public static BsonDocument By(IEnumerable<string> fields, bool ascending = true) {
				return new BsonDocument(fields.Select(field => new BsonElement(field, ascending?1:-1)));
			}

			/// <summary>
			/// Creates a sort expression by multiple fields in ascending order
			/// </summary>
			/// <param name="fields">A params array of fields or expressions to sort by</param>
			public static BsonDocument By(params string[] fields) {
				return By((IEnumerable<string>)fields, true);
			}

			/// <summary>
			/// Creates a sort expression by multiple fields in ascending order
			/// </summary>
			/// <param name="fields">A params array of fields or expressions to sort by</param>
			public static BsonDocument ByDescending(params string[] fields) {
				return By((IEnumerable<string>)fields, false);
			}

			/// <summary>
			/// Creates a sort expression to sort by ID
			/// </summary>
			/// <param name="ascending">True if ascending (default), false if descending</param>
			public static BsonDocument ById(bool ascending = true) {
				return new BsonDocument(RecordManager.ID_FIELD, ascending?1:-1);
			}
		}

		/// <summary>
		/// Generates an $unwind pipeline command against the specified array field
		/// </summary>
		/// <param name="arrayFieldPath">The name of the array field to unwind</param>
		public static BsonDocument Unwind(string arrayFieldPath) {
			return new BsonDocument("$unwind", "$" + arrayFieldPath);
		}
	}
}
