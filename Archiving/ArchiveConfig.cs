using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace SQLinqenlot.Archiving {
	/// <summary>
	/// simple class that basicly give you access to the ArchiveCnfig table - which is what is used by the program that ddoes the archiving
	/// </summary>
	public class ArchiveConfig : GenericTable {
		#region accessor
		public string ServerName {
			get { return (string)GetFieldValue("ServerName"); }
		}

		public string DatabaseName {
			get { return (string)GetFieldValue("DatabaseName"); }
		}

		//i need _col cause GT has a property call TabledName
		public string TableName_Col {
			get { return (string)GetFieldValue("TableName"); }
		}

		public string IDColumn {
			get { return (string)GetFieldValue("IDColumn"); }
		}

		public string AgeDeterminingColumn {
			get { return (string)GetFieldValue("AgeDeterminingColumn"); }
		}

		public string RecordArchiveMethodType {
			get { return (string)GetFieldValue("RecordArchiveMethod"); }
		}

		public string ColumnArchiveMethodType {
			get { return (string)GetFieldValue("ColumnArchiveMethod"); }
		}

		public string RecordArchiveInfo {
			get { return (string)GetFieldValue("RecordArchiveInfo"); }
			set { SetFieldValue("RecordArchiveInfo", value); }
		}

		public bool IsColumnDisbaled {
			get { return !this.ColumnDisabledDate.Equals(DateTime.MinValue); }
		}

		public DateTime ColumnDisabledDate {
			get { return (DateTime)GetFieldValue("ColumnDisabled"); }
		}

		public bool IsRecordDisbaled {
			get { return !this.RecordDisabledDate.Equals(DateTime.MinValue); }
		}

		public DateTime RecordDisabledDate {
			get { return (DateTime)GetFieldValue("RecordDisabled"); }
		}

		public string ColumnArchiveInfo {
			get { return (string)GetFieldValue("ColumnArchiveInfo"); }
			set { SetFieldValue("ColumnArchiveInfo", value); }
		}

		/// <summary>
		/// do we use compression
		/// </summary>
		public bool IsColumnCompressed {
			get { return ((string)this["IsCompressed"]).Contains("C"); }
		}

		public bool IsRecordCompressed {
			get { return ((string)this["IsCompressed"]).Contains("R"); }
		}

		/// <summary>
		/// do we squeeze together multiple columns into one value (via compression or freeze and thaw)
		/// </summary>
		public bool IsColumnSqueezed {
			get { return ((string)this["IsSqueezed"]).Contains("C"); }
		}

		public bool IsRecordSqueezed {
			get { return ((string)this["IsSqueezed"]).Contains("R"); }
		}

		//private ArchiveMethod mColumnArchiveMethod;
		//public ArchiveMethod ColumnArchiveMethod {
		//  get {
		//    if (mColumnArchiveMethod == null) {
		//    }

		//    return mColumnArchiveMethod;
		//  }
		//}

		private DateTime mColumnArchiveCutoff;
		public DateTime ColumnArchiveCutoff {
			get {
				if (mColumnArchiveCutoff == DateTime.MinValue) {
					int i = (int)GetFieldValue("ColumnAge");
					if (i == 0)
						mColumnArchiveCutoff = DateTime.MinValue;
					else
						mColumnArchiveCutoff = DateTime.Now.AddDays(-1 * i);
				}

				return mColumnArchiveCutoff;
			}
		}

		private DateTime mRecordArchiveCutoff;
		public DateTime RecordArchiveCutoff {
			get {
				if (mRecordArchiveCutoff == DateTime.MinValue) {
					int i = (int)GetFieldValue("RecordAge");
					if (i == 0)
						mRecordArchiveCutoff = DateTime.MinValue;
					else
						mRecordArchiveCutoff = DateTime.Now.AddDays(-1 * i);
				}

				return mRecordArchiveCutoff;
			}
		}

		public int ArchiveCount {
			get { return (int)GetFieldValue("ArchiveCount"); }
			set { SetFieldValue("ArchiveCount", value); }
		}

		private string[] mArchiveArchiveColumnList;
		public string[] ArchiveColumnList {
			get {
				if (mArchiveArchiveColumnList == null) {
					mArchiveArchiveColumnList = ((string)GetFieldValue("ColumnList")).Split(" ,".ToCharArray());
				}

				return mArchiveArchiveColumnList;
			}
		}

		#endregion

		#region constructor
		public ArchiveConfig() : base("ArchiveConfig", TDatabase.Syntac) { }
		#endregion

		#region static methods

		private static CachedQuery mCachedQuery;
		private static Dictionary<string, ArchiveConfig> mCache;
		public static ArchiveConfig Get(string TablePath) {
			if (mCachedQuery == null) {
				mCache = new Dictionary<string, ArchiveConfig>();
				CachedQuery.CacheUpdatedDelegate cb = new CachedQuery.CacheUpdatedDelegate(CacheCB);
				mCachedQuery = new CachedQuery(TDatabase.Syntac, "select * from ArchiveConfig", 180, cb);
				mCachedQuery.AllowNoRows = true;
			}

			mCachedQuery.CheckCache();
			if (mCache.ContainsKey(TablePath.ToLower()))
				return mCache[TablePath.ToLower()];
			return null;
		}


		public static void CacheCB(DataTable dt) {
			mCache.Clear();
			foreach (DataRow r in dt.Rows) {
				ArchiveConfig ac = new ArchiveConfig();
				ac.Load(r);
				mCache[ac.FullTablePath.ToLower()] = ac;
			}
		}

		public static string ConvertTablePathToFileName(string TablePath) {
			return Regex.Replace(TablePath, @"[:\\/""']", "_");
		}
		#endregion

	}
}
