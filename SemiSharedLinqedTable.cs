using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Reflection;

namespace SQLinqenlot {
	/// <summary>
	/// A SemiSharedLinqedTable is a LinqedTable that splits its data between the Shared database (shared, read-only) 
	/// and the Client database (writable).  It must be bound to a view that joins the two tables' data in a union,
	/// with the field IsClientData denoting whether the data belongs to the client.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class SemiSharedLinqedTable<TView, TClient> : LinqedTable<TView>
		where TView : LinqedTable<TView>
		where TClient : LinqedTable<TClient> {

		protected virtual string IsClientDataFieldName {
			get { return "IsClientData"; }
		}
		protected virtual bool IsClient {
			get {
				PropertyInfo prop = GetType().GetProperty(IsClientDataFieldName);
				return prop.GetValue(this, null).ToBool();
			}
		}
		private TClient _ClientTable;
		public TClient AsClientTable {
			get {
				if (_ClientTable == null) {
					if (IsClient)
						_ClientTable = LinqedTable<TClient>.Get(IDValue);
					else
						_ClientTable = null;
				}
				return _ClientTable;
			}
		}
		public static TView FromClientTable(TClient lt) {
			return LinqedTable<TView>.Get(lt.IDValue);
		}

		public override void SubmitChanges() {
			throw new NotImplementedException("Semi-shares are read-only");
		}
		public override void InsertOnSubmit() {
			throw new NotImplementedException("Semi-shares are read-only");
		}
		public override void DeleteOnSubmit() {
			throw new NotImplementedException("Semi-shares are read-only");
		}
	}
}
