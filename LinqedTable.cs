using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Reflection;
using System.ComponentModel;
using System.Xml;

namespace SQLinqenlot {
	using TPropertyDictionary = Dictionary<Type, Dictionary<string, PropertyInfo>>;

	public abstract class LinqedTable {
		#region Static members
		static LinqedTable() {
			__TableProperties = new TPropertyDictionary();
			__TableRelationships = new TPropertyDictionary();
		}
		private static TPropertyDictionary __TableProperties;
		/// <summary>
		/// Returns a collection of the properties that belong to the specified LinqedTable type.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		internal protected static Dictionary<string, PropertyInfo> GetProperties(Type type) {
			if (!typeof(LinqedTable).IsAssignableFrom(type))
				throw new ArgumentException("Type must be LinqedTable", "type");
			if (!__TableProperties.ContainsKey(type)) {
				Dictionary<string, PropertyInfo> props = new Dictionary<string, PropertyInfo>();
				foreach (PropertyInfo prop in type.GetProperties()) {
					object[] attrs = prop.GetCustomAttributes(typeof(ColumnAttribute), true);
					if (attrs.Length > 0)
						props[prop.Name] = prop;
				}
				__TableProperties[type] = props;
			}
			return __TableProperties[type];
		}

		private static TPropertyDictionary __TableRelationships;
		/// <summary>
		/// Returns a collection of the related objects that belong to the specified LinqedTable type.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		internal protected static Dictionary<string, PropertyInfo> GetRelationships(Type type) {
			if (!typeof(LinqedTable).IsAssignableFrom(type))
				throw new ArgumentException("Type must be LinqedTable", "type");
			if (!__TableRelationships.ContainsKey(type)) {
				Dictionary<string, PropertyInfo> props = new Dictionary<string, PropertyInfo>();
				foreach (PropertyInfo prop in type.GetProperties()) {
					object[] attrs = prop.GetCustomAttributes(typeof(AssociationAttribute), true);
					if (attrs.Length > 0)
						props[prop.Name] = prop;
				}
				__TableRelationships[type] = props;
			}
			return __TableRelationships[type];

		}
		/// <summary>
		/// Loads a LinqedTable with the specified ID.  Throws an exception if the record is not found.
		/// </summary>
		/// <param name="ID"></param>
		/// <returns></returns>
		public static T Get<T>(long ID) where T : LinqedTable {
			T result = TryGet<T>(ID);
			if (result == null)
				throw new RecordNotFoundException(typeof(T), ID);
			return result;
		}
		/// <summary>
		/// Loads a LinqedTable with the specified ID.  Returns null if the specified ID is null or the record is not found.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="ID"></param>
		/// <returns></returns>
		public static T TryGet<T>(long? ID) where T : LinqedTable {
			if (ID == null)
				return null;
			var context = LinqUtils.GetDataContext<T>();
			T instance = Activator.CreateInstance<T>();
			string CacheKey = string.Format("{0}:{1}", typeof(T).FullName, ID);
			// not in the cache; load it!
			var q = context.GetTable<T>().Where<T>(MakeFilter<T>(instance.IDPropertyName, ID));
			var result = q.SingleOrDefault();
			return result;
		}
		/// <summary>
		/// Makes a filter on a named column, rather than a literal property.
		/// </summary>
		/// <param name="propertyName"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		/// <remarks>
		/// Black magic at work here - this was lifted from 
		/// http://social.msdn.microsoft.com/forums/en-US/linqprojectgeneral/thread/df9dba6e-4615-478d-9d8a-9fd80c941ea2/
		/// </remarks>
		public static Expression<Func<T, bool>> MakeFilter<T>(string propertyName, object value) where T : LinqedTable {
			var type = typeof(T);

			var property = type.GetProperty(propertyName);

			var parameter = Expression.Parameter(type, "p");
			var propertyAccess = Expression.MakeMemberAccess(parameter, property);
			var constantValue = Expression.Constant(value);

			var equality = Expression.Equal(propertyAccess, constantValue);

			return Expression.Lambda<Func<T, bool>>(equality, parameter);
		}
		/// <summary>
		/// Converts an XML string to a LinqedTable of the appropriate type.
		/// </summary>
		/// <param name="xml"></param>
		/// <returns></returns>
		public static LinqedTable FromXML(string xml) {
			XmlDocument XmlDoc = new XmlDocument();
			XmlDoc.LoadXml(xml);
			string TypeName = XmlDoc.DocumentElement.GetAttribute("AssemblyName");
			LinqedTable lt;
			Type type;
			try {
				type = Type.GetType(TypeName);
				object o = Activator.CreateInstance(type);
				lt = (LinqedTable)o;
			} catch {
				throw new Exception("Xml string supplied is not for a LinqedTable.");
			}
			Dictionary<string, PropertyInfo> props = GetProperties(type);
			foreach (XmlNode node in XmlDoc.DocumentElement.ChildNodes) {
				string PropertyName = node.Name;
				Type PropertyType = props[PropertyName].PropertyType;
				if (PropertyType.IsGenericType && PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>)) {
					PropertyType = PropertyType.GetGenericArguments()[0];
				}
				object PropertyValue = Convert.ChangeType(node.InnerText, PropertyType);
				props[PropertyName].SetValue(lt, PropertyValue, null);
			}
			return lt;
		}

		public static bool operator ==(LinqedTable lt1, LinqedTable lt2) {
			if ((object)lt1 == null || (object)lt2 == null)
				return (object)lt1 == null && (object)lt2 == null;
			if (lt1.GetType() != lt2.GetType())
				return false;
			if (lt1.IDValue == 0 && lt2.IDValue == 0)
				return (object)lt1 == (object)lt2; // i.e. same instance!
			return lt1.IDValue == lt2.IDValue;
		}
		public static bool operator !=(LinqedTable lt1, LinqedTable lt2) {
			return !(lt1 == lt2);
		}

		public static Table<T> GetTable<T>() where T : LinqedTable {
			return LinqUtils.GetTable<T>();
		}
		#endregion

		#region Instance members
		private long mClientID;

		#region Constructor
		public LinqedTable() {
			// set the client for this class to the active client so that the DB connection string will be correct.
			mClientID = DBLocator.ActiveClientID;
		}
		#endregion

		#region Events
		public delegate void LinqedTableModified(LinqedTable lt);

		public event LinqedTableModified Saved;
		public event LinqedTableModified Deleted;

		internal void RaiseSavedEvent() {
			if (Saved != null)
				Saved(this);
		}

		internal void RaiseDeletedEvent() {
			if (Deleted != null)
				Deleted(this);
		}
		#endregion

		#region Overrides
		public override string ToString() {
			string Result = string.Format("[{0}]:", GetType().Name), Join = "";
			foreach (KeyValuePair<string, PropertyInfo> kvp in GetProperties(GetType())) {
				Result = string.Format("{0}{1}[{2}]={3}", Result, Join, kvp.Key, kvp.Value.GetValue(this, null));
				Join = ",";
			}
			return Result;
		}
		public override bool Equals(object obj) {
			LinqedTable lt2 = obj as LinqedTable;
			return this == lt2;
		}
		public override int GetHashCode() {
			return (int)(IDValue % int.MaxValue);
		}
		#endregion

		#region Identity
		public long IDValue {
			get { return DataUtils.ToLong(IDProperty.GetValue(this, null)); }
			set { IDProperty.SetValue(this, value, null); }
		}
		internal PropertyInfo IDProperty {
			get { return this.GetType().GetProperty(IDPropertyName); }
		}
		/// <summary>
		/// Returns the property name of the ID column for this table.  This is "ID" by default; descendant classes may override this.
		/// </summary>
		internal protected virtual string IDPropertyName {
			get { return "ID"; }
		}
		#endregion

		#region Data Persistence
		private LinqedTableDataContext _DataContext;
		/// <summary>
		/// Returns the data context for this table.
		/// </summary>
		public LinqedTableDataContext Context {
			get {
				if (_DataContext == null) {
					_DataContext = LinqUtils.GetDataContext(GetType(), mClientID);
				}
				return _DataContext;
			}
			private set {
				_DataContext = value;
			}
		}
		/// <summary>
		/// Returns the logical database to which this table belongs.
		/// </summary>
		public virtual TDatabase Database {
			get {
				// by default deduce the logical database from the data context
				string ContextName = LinqUtils.GetDataContextType(GetType()).Name;
				string LogicalDBName = ContextName.Replace("DataClassesDataContext", "");
				return (TDatabase)Enum.Parse(typeof(TDatabase), LogicalDBName);
			}
		}
		/// <summary>
		/// Submits changes made to this and any other related objects.
		/// </summary>
		public virtual void SubmitChanges() {
			Context.SubmitChanges();
		}
		/// <summary>
		/// Inserts this object into the database and submits changes made to any other related objects.
		/// </summary>
		public virtual void Insert() {
			InsertOnSubmit();
			SubmitChanges();
		}
		/// <summary>
		/// Notifies the data context that this object should be inserted when a call is made to SubmitChanges.
		/// </summary>
		public virtual void InsertOnSubmit() {
			if (IsAttached)
				return;
			if (!Context.GetChangeSet().Inserts.Contains(this))
				Context.GetTable(GetType()).InsertOnSubmit(this);
		}
		/// <summary>
		/// Deletes this object from the database and submits changes made to any other related objects.
		/// </summary>
		public virtual void Delete() {
			DeleteOnSubmit();
			SubmitChanges();
		}
		/// <summary>
		/// Notifies the data context that this object should be deleted when a call is made to SubmitChanges.
		/// </summary>
		public virtual void DeleteOnSubmit() {
			if (!IsAttached)
				return;
			if (!Context.GetChangeSet().Deletes.Contains(this)) {
				Context.GetTable(GetType()).DeleteOnSubmit(this);
			}
		}
		/// <summary>
		/// Does some black magic to work out if this object has already been attached to the data context.
		/// </summary>
		/// <returns></returns>
		internal bool IsAttached {
			get {
				var table = Context.GetTable(GetType());
				var o = table.GetOriginalEntityState(this);
				return o != null;
			}
		}
		/// <summary>
		/// This method is called before this object is inserted into the database.
		/// </summary>
		internal protected virtual void BeforeInsert() {
			BeforeSave();
		}
		/// <summary>
		/// This method is called before this object is updated to the database.
		/// </summary>
		internal protected virtual void BeforeUpdate() {
			BeforeSave();
		}
		/// <summary>
		/// This method is called by the default implementations of BeforeInsert and BeforeUpdate.
		/// If a subclass overrides BeforeInsert or BeforeUpdate, BeforeSave will not automatically be called by insert or update events respectively.
		/// </summary>
		internal protected virtual void BeforeSave() { }
		/// <summary>
		/// This method is called before this object is deleted from the database.
		/// </summary>
		internal protected virtual void BeforeDelete() { }
		/// <summary>
		/// This method is called after this object is inserted into the database.
		/// </summary>
		internal protected virtual void AfterInsert() {
			AfterSave();
		}
		/// <summary>
		/// This method is called after this object is updated to the database.
		/// </summary>
		internal protected virtual void AfterUpdate() {
			AfterSave();
		}
		/// <summary>
		/// This method is called by the default implementations of AfterInsert and AfterUpdate.
		/// If a subclass overrides AfterInsert or AfterUpdate, AfterSave will not automatically be called by insert or update events respectively.
		/// </summary>
		internal protected virtual void AfterSave() { }
		/// <summary>
		/// This method is called after this object is deleted from the database.
		/// </summary>
		internal protected virtual void AfterDelete() { }
		#endregion

		#region Audit
		/// <summary>
		/// Returns whether this LinqedTable must have rows inserted into the local audit logs when values in the table are 
		/// changed.  This property is false by default; subclasses may override this property to enable Auto Audit.
		/// </summary>
		internal protected virtual bool AutoAudit {
			get { return false; }
		}
		#endregion

		#region Web services
		/// <summary>
		/// Converts this LinqedTable to XML
		/// </summary>
		/// <returns></returns>
		public virtual string ToXML() {
			XmlDocument XmlDoc = new XmlDocument();
			XmlElement RootElement = XmlDoc.CreateElement("LinqedTable");
			RootElement.SetAttribute("AssemblyName", GetType().AssemblyQualifiedName);
			// all internal elements
			foreach (KeyValuePair<string, PropertyInfo> kvp in GetProperties(GetType())) {
				object Value = kvp.Value.GetValue(this, null);
				if (Value == null || Value == DBNull.Value)
					continue;
				XmlNode node = XmlDoc.CreateNode(XmlNodeType.Element, kvp.Key, "");
				node.InnerText = Value.ToString();
				RootElement.AppendChild(node);
			}
			// all related objects - TODO...
			bool DoRelatedObjects = false;
			if (DoRelatedObjects) {
				foreach (KeyValuePair<string, PropertyInfo> kvp in GetRelationships(GetType())) {
					LinqedTable RelatedObject = kvp.Value.GetValue(this, null) as LinqedTable;
					if (RelatedObject == null)
						continue; // this is true whether it's null because the value is null or because it's not a LinqedTable
					XmlNode node = XmlDoc.CreateNode(XmlNodeType.Element, kvp.Key, "");
					// continue...
				}
			}
			XmlDoc.AppendChild(RootElement);
			return XmlDoc.OuterXml;
		}


		#endregion

		#endregion

		#region Nested Exceptions
		public class RecordNotFoundException : Exception {
			public RecordNotFoundException(Type type, long ID) :
				base(string.Format("Could not load a LinqedTable of type {0} with ID {1}.", type.Name, ID)) {
			}
		}
		#endregion
	}

	public class LinqedTable<T> : LinqedTable where T : LinqedTable<T> {
		#region Statics
		public static T Get(long ID) {
			return Get<T>(ID);
		}
		public static T TryGet(long? ID) {
			return TryGet<T>(ID);
		}
		public static Table<T> GetTable() {
			return LinqUtils.GetTable<T>();
		}
		public static Table<T> GetTable(long ClientID) {
			return LinqUtils.GetTable<T>(ClientID);
		}
		public static IQueryable<T> Where(Expression<Func<T, bool>> predicate) {
			return GetTable().Where(predicate);
		}
		public static IQueryable<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) {
			return GetTable().Select(selector);
		}
		public static bool Any(Expression<Func<T, bool>> predicate) {
			return GetTable().Any(predicate);
		}
		public static bool All(Expression<Func<T, bool>> predicate) {
			return GetTable().All(predicate);
		}

		public static bool operator ==(LinqedTable<T> lt1, LinqedTable<T> lt2) {
			if ((object)lt1 == null || (object)lt2 == null)
				return (object)lt1 == null && (object)lt2 == null;
			if (lt1.GetType() != lt2.GetType())
				return false;
			if (lt1.IDValue == 0 && lt2.IDValue == 0)
				return (object)lt1 == (object)lt2; // i.e. same instance!
			return lt1.IDValue == lt2.IDValue;
		}
		public static bool operator !=(LinqedTable<T> lt1, LinqedTable<T> lt2) {
			return !(lt1 == lt2);
		}
		#endregion

		#region Overrides
		public override void InsertOnSubmit() {
			if (IsAttached)
				return;
			if (!Context.GetChangeSet().Inserts.OfType<T>().ToList().Contains((T)this))
				Context.GetTable(GetType()).InsertOnSubmit(this);
		}
		public override void DeleteOnSubmit() {
			if (!IsAttached)
				return;
			if (!Context.GetChangeSet().Deletes.OfType<T>().ToList().Contains((T)this))
				Context.GetTable(GetType()).DeleteOnSubmit(this);
		}
		public override int GetHashCode() {
			return base.GetHashCode(); // have to do this to get rid of the compiler warning
		}
		public override bool Equals(object obj) {
			return base.Equals(obj); // have to do this to get rid of the compiler warning
		}
		#endregion
	}
}
