using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace SQLinqenlot {
	/// <summary>
	/// Summary description for Client.
	/// </summary>
	public class GenericClient : GenericTable {
		static GenericClient() {
			__EntsByFeatureUser = new Dictionary<string, Dictionary<string, bool>>();
		}
		public enum EntitlementType {
			DefaultPositive = 1,
			DefaultNegative = 2
		}

		public GenericClient()
			: base("Client", TDatabase.Shared) {
		}

		#region accessors
		private Dictionary<string, string> mOptions = null;
		/// <summary>
		/// a hash that gets populated by the options column ib the client table.  WHich contains a comma sep list of field=value pairs
		/// </summary>
		public Dictionary<string, string> Options {
			get {
				if (mOptions == null)
					mOptions = TextUtility.FieldValuePairs((string)this["Options"]);

				return mOptions;
			}
		}

		public string Name {
			get { return (string)this["Name"]; }
			set { this["Name"] = value; }
		}

		public string Code {
			get {
				if ((string)this["Code"] == "") {
					return Name;
				}
				return (string)this["Code"];
			}
			set { this["Code"] = value; }
		}

		public string Type {
			get {
				if ((string)this["Type"] == "") {
					return Name;
				}
				return (string)this["Type"];
			}
			set { this["Type"] = value; }
		}

		public string LongName {
			get {
				if ((string)this["LongName"] == "") {
					return (string)this["Name"];
				}

				return (string)this["LongName"];
			}
			set { this["LongName"] = value; }
		}

		public DateTime CreatedDate {
			get { return DataUtils.ToDate(this["CreatedDate"]).GetValueOrDefault(); }
			set { this["CreatedDate"] = value; }
		}


		public byte Active {
			get { return DataUtils.ToByte(this["Active"]); }
			set { this["Active"] = value; }
		}

		public string ReadOnlyLogin {
			get { return this["ReadOnlyLogin"].BlankIfNull().ToString(); }
			set { this["ReadOnlyLogin"] = value; }
		}

		public string ReadOnlyPassword {
			get { return EncryptionUtil.DecryptTripleDES(this["ReadOnlyPassword"].BlankIfNull().ToString()); }
			set { this["ReadOnlyPassword"] = EncryptionUtil.EncryptTripleDES(value); }
		}

		public string ReadWriteLogin {
			get { return this["ReadWriteLogin"].BlankIfNull().ToString(); }
			set { this["ReadWriteLogin"] = value; }
		}

		public string ReadWritePassword {
			get { return EncryptionUtil.DecryptTripleDES(this["ReadWritePassword"].BlankIfNull().ToString()); }
			set { this["ReadWritePassword"] = EncryptionUtil.EncryptTripleDES(value); }
		}
		#endregion accessors

		private static CachedQuery mCachedQuery;
		private static List<GenericClient> mClientCache;
		public static List<GenericClient> ClientCache {
			get {
				if (mCachedQuery == null) {
					mClientCache = new List<GenericClient>();
					CachedQuery.CacheUpdatedDelegate cb = new CachedQuery.CacheUpdatedDelegate(CacheCB);
					mCachedQuery = new CachedQuery(TDatabase.Shared, "select * from Client order by ID", 60, cb);
				}

				mCachedQuery.CheckCache();
				return mClientCache;
			}
		}

		/// <summary>
		/// Default does not force refresh of cached list
		/// </summary>
		/// <returns></returns>
		public static List<GenericClient> GetActiveClients() {
			return GetActiveClients(false);
		}

		/// <summary>
		/// Get list of active clients.
		/// </summary>
		/// <param name="ForceRefresh">to get whats in client table now.</param>
		/// <returns></returns>
		public static List<GenericClient> GetActiveClients(bool ForceRefresh) {
			if (ForceRefresh)
				mCachedQuery = null;

			List<GenericClient> ar = new List<GenericClient>();
			foreach (GenericClient c in ClientCache) {
				if (c.Active == (byte)TClientStatus.Active)
					ar.Add(c);
			}
			return ar;
		}

		public static void CacheCB(DataTable dt) {
			mClientCache.Clear();

			foreach (DataRow r in dt.Rows) {
				GenericClient c = new GenericClient();
				c.Load(r);
				mClientCache.Add(c);
			}
		}

		protected override long ProtectedCreateRecord(bool force) {
			// find the largest ID
			string q = "select max(ID) from Client";
			short MaxID = DataUtils.ToShort(sqlUtil.ExecuteScalarResultSetSQLQuery(q));
			ID = MaxID + 1;
			CreatedDate = DateTimeUtility.ServerDate();
			mCachedQuery = null;
			return base.ProtectedCreateRecord(force);
		}

		public static GenericClient GetByID(long ClientID) {
			foreach (GenericClient c in ClientCache) {
				if (c.ID == ClientID)
					return c;
			}

			return null;
		}

		public static GenericClient GetByName(string ClientName) {
			ClientName = (ClientName ?? "").ToLower();
			return ClientCache.Where(c => c.Name.ToLower() == ClientName).SingleOrDefault();
		}

		public static SortedList<string, string> GetAllClientNames() {
			SortedList<string, string> Result = new SortedList<string, string>();
			foreach (GenericClient c in ClientCache) {
				Result.Add(c.Name, c.Name);
			}
			return Result;
		}

		private static Dictionary<string, Dictionary<string, bool>> __EntsByFeatureUser;
		/// <summary>
		/// is client entitled to  feature. Entitlements work two ways - eaither by Default On or by Default Off.
		/// If default is on, then A feature is turned off by having the client name in the list for this "feature/NotEntiled" app parm 
		/// in the shared table or having this feature appear in the client's AppParm "Setup/feature_NotEntitled" (which is created by SetupClient)
		/// if the default is off, the a client has a feature truned on by having the client name in the app parm "feature/Entiled" in the
		/// shared table or the or having this feature appear in the client's AppParm "Setup/feature_Entitled"
		/// Most features in the system are by default entitled
		/// </summary>
		/// <param name="c"></param>
		/// <returns></returns>
		public static bool Entitled(string feature, GenericClient c, EntitlementType EntType) {
			string cname = c.Name.ToLower();

			string ParmName;
			bool ParmAction;

			Dictionary<string, bool> ents = __EntsByFeatureUser[feature];
			if (ents == null) {
				if (EntType == EntitlementType.DefaultPositive) {
					ParmName = "ClientsNotEntitled";
					ParmAction = false;
				} else {
					ParmName = "ClientsEntitled";
					ParmAction = true;
				}

				ents = new Dictionary<string, bool>();
				__EntsByFeatureUser[feature] = ents;

				//this parm that should only exist on shared level
				string str = ApplicationParameters.GetParm(feature, ParmName);
				if (str != null) {
					str = str.ToLower();
					string[] parts = str.Split(",".ToCharArray());
					foreach (string name in parts) {
						ents[name] = ParmAction;
					}
				}
			}

			//a null indicates we still have not checked the Setup paramaters on the client level
			object o = ents[cname];
			if (o != null)
				return (bool)o;

			if (EntType == EntitlementType.DefaultPositive) {
				ParmName = "_NotEntitled";
				ParmAction = false;
			} else {
				ParmName = "_Entitled";
				ParmAction = true;
			}

			//now check client setup paramaters
			string str2 = ApplicationParameters.GetParm("Setup", feature + ParmName);
			if (str2 != null)
				ents[cname] = ParmAction;
			else
				ents[cname] = !ParmAction;

			return (bool)ents[cname];
		}

		public override string ToString() {
			return Name;
		}
	}
}
