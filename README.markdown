# Mongol Project
Welcome! Mongol is a very simple wrapper library for 10gen's [Official MongoDB C#/.NET Driver](http://www.mongodb.org/display/DOCS/CSharp+Language+Center) that helps shortcut some repetitive tasks when building applications that use MongoDB to store documents in strongly-typed collections.

## Quick Start
The easiest way to get started with Mongol is to

1. Add Mongol to your project using [Nuget](http://docs.nuget.org/docs/start-here/managing-nuget-packages-using-the-dialog)
2. Add the **Mongol.Url** appSetting to your application .config
```xml
<appSettings>
    <add key="Mongol.Url" value="mongodb://hostname/database?options" /> 
</appSettings>
```
3. Create the classes that you wish to store and retrieve with MongoDB.  This can be done of two ways:
    * Create your own POCO classes and add the [BsonId] attribute to your uniquely identifying field if it isn't  called Id, id, or _id.
```c#
public class Person {
    [BsonId]
    public Guid PersonId { get; set; }
    public string Name { get; set; }
}
```
    * Inherit from **_Mongol.Record_** or **_Mongol.TimestampedRecord_** which already has
```c#
[BsonId(IdGenerator = typeof(StringObjectIdGenerator))]
public virtual string Id { get; set; }
```

Now you can create an instance of **RecordManager\<Person\>** and go to town.  For example:

```c#
    var personManager = new RecordManager<Person>();
    Person person = personManager.GetById(person.Id);
    person.Name = "Updated Name";
    personManager.Save(person);
```

## What Mongol _does not_ do

Mongol does not promise to take out your trash or walk you dog. If it improves your marriage, then it's probably because it saved you a few extra minutes at work. 

Everything that can be done with Mongol can be done directly using the 10Gen Driver.  Mongol simply wraps some of the common operations I found myself repeating and makes some of those repetitive tasks a little bit less... repetitive... :) 

Mongol also provides some _very limited_ prescriptive guidance when starting up a new project using MongoDB. 

## What Mongol _does_ do

Mongol provides a few features on top of the MongoDB driver, all of which can be used independently of each other.  Feel free to use the features you want and ignore the rest, Mongol won't get it's feelings hurt.

Features offered by Mongol:

* A simple [Repository Pattern](http://martinfowler.com/eaaCatalog/repository.html) wrapper for MongoDB collections exposing the most common CRUD functionality for strongly typed documents 
```c#
internal class PersonManager : RecordManager<Person> {
    // Public methods inherited from RecordManager<T>
    IQueryable<T> AsQueryable { get; }
    void DeleteById(object id);
    T GetById(object id);
    IEnumerable<T> GetManyByIds(IEnumerable<object> ids);
    bool Save(T record);
    void BatchInsert(IEnumerable<T> records);

    // Protected methods inherited from RecordManager<T>
    long Count(IMongoQuery criteria = null);
    IEnumerable<T> Find(IMongoQuery criteria, IMongoSortBy sort = null, int? skip = null, int? limit = null);
    T FindSingle(IMongoQuery criteria);
    void Initialize(); // For once-per application runtime maintenance like Ensuring Indexes, Purging Data, Setting Conventions
    void DropCollection();
    T FindOneAndModify(IMongoQuery criteria, IMongoUpdate update, IMongoSortBy sortBy = null, bool returnModifiedVersion = true);
    T FindOneAndRemove(IMongoQuery criteria, IMongoSortBy sortBy = null);
    IEnumerable<T> EnumerateAndModify(IMongoQuery criteria, IMongoUpdate update, IMongoSortBy sortBy = null, bool returnModifiedVersion = true);
    IEnumerable<T> EnumerateAndRemove(IMongoQuery criteria, IMongoSortBy sortBy = null);
    long UpdateMany(IMongoQuery criteria, UpdateBuilder update, bool asUpsert = false);
    long DeleteMany(IMongoQuery criteria);
    void EnsureIndex(IMongoIndexKeys keys, IMongoIndexOptions options);
}
```
* Lambda-based property-name resolution for building MongoDB queries without using magic strings 
```c#
public IEnumerable<Person> GetByLastName(string LastName) {
  // Instead of magic strings like this:
  return Find(Query.EQ("LastName", LastName));
  // Use Lambdas for Compile-time safety like this:
  return Find(Query.EQ(PropertyName(p => p.LastName), LastName));
  // Also works for collection members by using .Single()
  string LastNameField = PropertyName(p.Children.Single().LastName; // Evaluates to "Children.LastName"
  return Find(Query.EQ(LastNameField, LastName)); // [NOTE: Returns the parent document]
}
```

* Simple connection-string configuration using a single appSetting
```xml
<appSettings>
  <add key="Mongol.Url" value="mongodb://localhost/test?safe=true"/>
</appSettings>
```

* A manager factory to encapsulate/cache construction of new repository instances

```c#
PersonManager personManager = ManagerFactory.GetManager<PersonManager>();
```

* A base-class from which strongly-typed documents can optionally inherit, providing string-typed Ids and automatic **_id** field population if saved as **null**.

```c#
public class Person : Record {
  public string FirstName { get; set; }
  public string LastName { get; set; }
  public Address Address { get; set; }
  public Person[] Children { get; set; }
}

// Inherited from Record
[BsonId(IdGenerator = typeof(StringObjectIdGenerator))]
public virtual string Id { get; set; }
```

* Another base-class (and interface) from which documents can inherit providing automatic maintenance of Creation/Modification timestamps.

```c#
public class Person : ITimeStampedRecord {
  #region ITimeStampedRecord Members
  public DateTime CreatedDate { get; set; }
  public DateTime ModifiedDate { get; set; }
  #endregion
}
```