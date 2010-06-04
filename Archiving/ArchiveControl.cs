using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SQLinqenlot.Archiving {
	/// <summary>
	/// two classes used to read in and process the archive control table.    The archive control table basicly contain information on data that was
	/// already archived
	/// </summary>
	[Serializable()]
	public class ArchiveControl : GenericTable {
		public enum ArchiveTypeEnum {
			Record = 'R',
			Column = 'C',
		}

		#region propeties

		public string TablePath {
			get { return (string)GetFieldValue("TablePath"); }
			set { SetFieldValue("TablePath", value); }
		}

		public ArchiveTypeEnum ArchiveType {
			get { return (ArchiveTypeEnum)((string)GetFieldValue("ArchiveType")).ToCharArray()[0]; }
			set { SetFieldValue("ArchiveType", System.Convert.ToString((char)value)); }
		}

		private object[] mRange;
		public object[] Range {
			get {
				if (mRange == null) {
					mRange = new object[2];
					string s = (string)GetFieldValue("Range");
					if (s.Length == 0) {
						mRange[0] = null;
						mRange[1] = null;
						return mRange;
					}

					string[] parts = s.Split("|".ToCharArray());
					if (parts.Length != 2)
						throw new Exception("invalid range for ARchiveConfig record " + this.ID);

					if (ArchiveType == ArchiveTypeEnum.Column) {
						mRange[0] = System.Convert.ToDateTime(parts[0]);
						mRange[1] = System.Convert.ToDateTime(parts[1]);
					} else {
						mRange[0] = System.Convert.ToInt64(parts[0]);
						mRange[1] = System.Convert.ToInt64(parts[1]);
					}
				}
				return mRange;
			}
		}

		public string ArchiveMethodType {
			get { return (string)GetFieldValue("ArchiveMethod"); }
			set { SetFieldValue("ArchiveMethod", value); }
		}

		public string ArchiveInfo {
			get { return (string)GetFieldValue("ArchiveInfo"); }
			set { SetFieldValue("ArchiveInfo", value); }
		}

		/// <summary>
		/// do we use compression
		/// </summary>
		public bool IsCompressed {
			get { return (bool)GetFieldValue("IsCompressed"); }
			private set { SetFieldValue("IsCompressed", value); }
		}

		/// <summary>
		/// do we squeeze together multiple columns into one value (via compression or freeze and thaw)
		/// </summary>
		public bool IsSqueezed {
			get { return (bool)GetFieldValue("IsSqueezed"); }
			private set { SetFieldValue("IsSqueezed", value); }
		}


		private ArchiveMethod mArchiveMethod;
		public ArchiveMethod ArchiveMethod {
			get {
				if (mArchiveMethod == null)
					mArchiveMethod = this.CreateArchiveMethodInstance();

				return mArchiveMethod;
			}
		}

		private ArchiveConfig mArchiveConfg = null;
		public ArchiveConfig cfg {
			get {
				if (mArchiveConfg == null) {
					mArchiveConfg = ArchiveConfig.Get(this.TablePath);
				}

				return mArchiveConfg;
			}
		}

		//cache 
		private static Dictionary<string, ArchiveControlList> mArchive = new Dictionary<string, ArchiveControlList>();

		private static SqlUtil SQL {
			get { return SqlUtil.Get(TDatabase.Syntac); }
		}

		#endregion

		private ArchiveControl() : base("ArchiveControl", TDatabase.Syntac) { }

		public ArchiveControl(DataRow r)
			: this() {
			this.Load(r);

		}

		public ArchiveControl(ArchiveConfig cfg, ArchiveControl.ArchiveTypeEnum type)
			: this() {
			ArchiveControl ctl = this;

			ctl.ArchiveType = type;
			ctl.TablePath = cfg.FullTablePath;
			ctl.mArchiveConfg = cfg;

			if (type == ArchiveControl.ArchiveTypeEnum.Record) {
				ctl.ArchiveMethodType = cfg.RecordArchiveMethodType;
				ctl.ArchiveInfo = cfg.RecordArchiveInfo;
				ctl.IsCompressed = cfg.IsRecordCompressed;
				ctl.IsSqueezed = cfg.IsRecordSqueezed;
			} else {
				ctl.ArchiveMethodType = cfg.ColumnArchiveMethodType;
				ctl.ArchiveInfo = cfg.ColumnArchiveInfo;
				ctl.IsCompressed = cfg.IsColumnCompressed;
				ctl.IsSqueezed = cfg.IsColumnSqueezed;
			}
		}

		public ArchiveMethod CreateArchiveMethodInstance() {
			if (this.ArchiveMethodType == "FileSystem")
				return new FileSystemArchiveMethod(this);

			if (this.ArchiveMethodType == "Sql")
				return new SqlArchiveMethod(this);

			throw new Exception("not defined archive method type " + this.ArchiveMethodType);
		}

		public override long SaveRecord() {
			SetFieldValue("Range", mRange[0].ToString() + "|" + mRange[1].ToString());
			return base.SaveRecord();
		}

		#region static methids
		//for date archive
		public static ArchiveControl Get(string TablePath, DateTime SomeTime) {
			ArchiveControlList l = GetControlList(TablePath);
			if (l == null)
				return null;

			return l.Find(SomeTime);
		}


		//for record archive
		public static ArchiveControl Get(string TablePath, long id) {
			ArchiveControlList l = GetControlList(TablePath);
			if (l == null)
				return null;

			return l.Find(id);
		}

		//used only by archiver
		public static ArchiveControl Get(string TablePath, string ArchiveInfo, ArchiveControl.ArchiveTypeEnum ArchiveType) {
			ArchiveControlList l = GetControlList(TablePath);
			if (l == null)
				return null;

			ArchiveControl found = null;
			foreach (ArchiveControl a in l.mArchiveList) {
				if (a.ArchiveType != ArchiveType)
					continue;

				if (a.ArchiveInfo == ArchiveInfo) {
					//once a particular archiveinfo is used, it can not be reused latter on since we will mess up the ranges
					if (found == null) {
						found = a;
						continue;
					} else
						throw new Exception(String.Format("trying to reuse archiveinfo for {0}, info {1}", TablePath, ArchiveInfo));
				}
			}

			return found;
		}

		public static ArchiveControl GetMostRecent(string TablePath, ArchiveControl.ArchiveTypeEnum type) {
			ArchiveControlList acl = GetControlList(TablePath);
			ArchiveControl found = null;

			foreach (ArchiveControl ac in acl.mArchiveList) {
				if (ac.ArchiveType != type)
					continue;

				if (found == null)
					found = ac;
				else {
					if (type == ArchiveTypeEnum.Column) {
						if ((DateTime)ac.Range[1] > (DateTime)found.Range[1])
							found = ac;
					} else {
						if ((long)ac.Range[1] > (long)found.Range[1])
							found = ac;
					}
				}
			}

			return found;
		}

		internal static ArchiveControlList GetControlList(string TablePath) {
			LoadArchiveControl();

			string key = TablePath.ToLower();
			ArchiveControlList l = mArchive[key];
			if (l != null)
				return l;
			else
				return null;
		}

		public static bool Exists(string TablePath) {
			if (GetControlList(TablePath) == null)
				return false;
			else
				return true;
		}

		#endregion

		#region cached queury
		private static CachedQuery mCachedQuery;
		private static void LoadArchiveControl() {
			if (mCachedQuery == null) {
				CachedQuery.CacheUpdatedDelegate cb = new CachedQuery.CacheUpdatedDelegate(CacheCB);
				mCachedQuery = new CachedQuery(TDatabase.Syntac, "select * from ArchiveControl order by id asc", 180, cb);
				mCachedQuery.AllowErrors = 2;
			}

			mCachedQuery.CheckCache();
		}

		private static void CacheCB(DataTable dt) {
			mArchive.Clear();
			ArchiveControlList archiveList;

			string k;
			foreach (DataRow r in dt.Rows) {
				ArchiveControl a = new ArchiveControl(r);

				k = a.TablePath.ToLower();
				archiveList = (ArchiveControlList)mArchive[k];
				if (archiveList == null) {
					archiveList = new ArchiveControlList();
					mArchive[k] = archiveList;
				}

				archiveList.Add(a);
			}
		}
		#endregion
	}

	internal class ArchiveControlList {
		///i choose not to have two list (one for dates and one for id).  It could make the searchr esultion a
		///bit faster, but i did not t hink it was worth it since we arealready going to an archive the time
		///we spend if not significate relative to the IO
		internal List<ArchiveControl> mArchiveList;
		object[] mRanges = new object[2];

		DateTime[] mDateRange = new DateTime[2];
		long[] mIdRange = new long[2];

		string mTablePath;

		internal ArchiveControlList() {
			mArchiveList = new List<ArchiveControl>();
			mDateRange[0] = DateTime.MaxValue;
			mIdRange[0] = System.Int64.MaxValue;
		}

		internal void Add(ArchiveControl a) {
			mTablePath = a.TablePath;

			mArchiveList.Add(a);

			//see if this record expands the borders of the archive
			if (a.ArchiveType == ArchiveControl.ArchiveTypeEnum.Column) {
				if ((DateTime)a.Range.GetValue(0) < mDateRange[0])
					mDateRange[0] = (DateTime)a.Range[0];

				if ((DateTime)a.Range.GetValue(1) > mDateRange[1])
					mDateRange[1] = (DateTime)a.Range[1];
			} else {
				if ((long)a.Range.GetValue(0) < mIdRange[0])
					mIdRange[0] = (long)a.Range[0];

				if ((long)a.Range.GetValue(1) > mIdRange[1])
					mIdRange[1] = (long)a.Range[1];
			}
		}

		internal ArchiveControl Find(DateTime t) {
			// we asuume that only dates less than the max date are archived
			if (t > mDateRange[1])
				return null;

			object[] r;
			foreach (ArchiveControl a in mArchiveList) {
				if (a.ArchiveType != ArchiveControl.ArchiveTypeEnum.Column)
					continue;

				r = a.Range;
				if (t >= (DateTime)r[0] && t <= (DateTime)r[1])
					return a;
			}

			return null;
		}

		internal ArchiveControl Find(long id) {
			// we asuume that only IDs less than the max date are archived
			if (id > mIdRange[1])
				return null;

			object[] r;
			foreach (ArchiveControl a in mArchiveList) {
				if (a.ArchiveType != ArchiveControl.ArchiveTypeEnum.Record)
					continue;

				r = a.Range;
				if (id >= (long)r[0] && id <= (long)r[1])
					return a;
			}

			return null;
		}
	}

}
