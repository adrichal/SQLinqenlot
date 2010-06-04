using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using SQLinqenlot.Archiving;
using System.Diagnostics;

namespace SQLinqenlot {
	[Serializable()]
	public class GenericField {
		public int mType;
		public short mLength;
		public bool mReadOnly = false;
		public bool mEncrypted = false;
	}

	/// <summary>
	/// A class to preform CRUD on any table
	/// </summary>
	[Serializable()]
	public class GenericTable {
		public enum MyDbTypes {
			Long = 127,
			String = 167,
			Bool = 104,
			smallDate = 58,
			Date = 61,
			Char = 175,
			Byte = 48,
			Short = 52,
			Integer = 56,
			Float = 59,
			Money = 60,
			Text = 35,
			Image = 34,
			Double = 62,
			nVarChar = 231,
			nText = 99,
		}

		public const string GT_AUTORECOVER = "GT_Auto";
		private static Dictionary<string, Dictionary<string, GenericField>> mAllTableDefs;
		private static object[] mTypedNullObject;
		private static SqlDbType[] mSqlDbType;
		private static Dictionary<string, bool> mHasIdentityCache = new Dictionary<string, bool>();
		private static Dictionary<string, string> mIdColumnHash = new Dictionary<string, string>();

		#region properties
		private string mFullTablePath;
		public bool ReadOnly = false;
		private bool mTruncateStrings = false;
		private string mTableName;
		protected bool mNewRecord = false;
		private ArchivedData mArchivedRecord;
		private ArchivedData mArchivedColumn;

		//if the is true is subclass, the write method will also check ExitingRecordId methid
		//and throw exception is it exists
		protected virtual bool AllowDirectCreate {
			get { return true; }
		}

		private bool mIgnoreUnknownFields = false;
		/// <summary>
		/// Returns or sets whether additional fields that are not in the DB table can be used.
		/// </summary>
		public virtual bool IgnoreUnknownFields {
			get { return mIgnoreUnknownFields; }
			set { mIgnoreUnknownFields = value; }
		}

		private bool mIgnoreMissingFields = false;
		/// <summary>
		/// Returns or sets whether to ignore fields that are defined in the table but not supplied in the load.  If true, such fields will return DB Null.
		/// </summary>
		public virtual bool IgnoreMissingFields {
			get { return mIgnoreMissingFields; }
			set { mIgnoreMissingFields = value; }
		}

		private SqlUtil mSqlUtil;
		public SqlUtil sqlUtil {
			get { return mSqlUtil; }
		}

		public Dictionary<string, GenericField> mTableDef;
		private Dictionary<string, object> mChangeList;

		private string mWriteStmt;
		private string mDeleteStmt;
		private bool mHasIdentity = false;
		private string mIdColumn;
		protected string IDColumnName {
			get { return mIdColumn; }
		}

		private DataRow mDataRow;
		public DataRow DataRow {
			get { return mDataRow; }
		}

		private Dictionary<string, object> mData;
		public Dictionary<string, object> Data {
			get { return mData; }
		}
		/// <summary>
		/// Returns whether this record has ever been persisted.
		/// </summary>
		public bool IsNew {
			get { return mNewRecord; }
		}
		/// <summary>
		/// Sets the data values of the current object to be identical to the object supplied.
		/// </summary>
		/// <param name="gt">The generic table to be cloned</param>
		/// <remarks>
		/// Use with caution!  This changes the current object!  It is intended for subclasses to allow downcasting from items that were created
		/// as superclasses.
		/// </remarks>
		protected void MakeMeClone(GenericTable gt) {
			this.ID = gt.ID;
			this.mData = gt.mData;
			this.mNewRecord = gt.mNewRecord;
			this.mChangeList = new Dictionary<string, object>(gt.mChangeList);
		}

		private string mAutoRecover;
		private string AutoRecover {
			set { mAutoRecover = value; }
			get { return mAutoRecover; }
		}

		#region Automatic Audit History stuff
		/// <summary>
		/// Indicates if this table must keep an audit history.  Default is false; descendant classes must override to keep audit history.
		/// </summary>
		public bool AutomaticAuditHistory = false;


		/// <summary>
		/// stuff that will get added to the audit when GT creates an audit (if auto auditing is truned on)
		/// after the IO is done, the list is cleared, so the next IO wont have this additional info
		/// </summary>
		public AuditFields AdditionalAuditData = null;

		private TDatabase mAuditHistoryDB = TDatabase.Client;
		/// <summary>
		/// Returns the database in which the audit history table is stored.
		/// By default this is the "Audit" database; descendant classes may override this. 
		/// </summary>
		public TDatabase AuditHistoryDatabase {
			get {
				return mAuditHistoryDB;
			}
			set {
				mAuditHistoryDB = value;
			}
		}

		#endregion

		private DateTime mLoadTime;
		private long mLastAuditID = 0;
		private bool mConcurrencyCheck;
		/// <summary>
		/// consurncey checking is implemented using the Audit feature. If auditing is not on 
		/// this feature will exception out.  Before an update is made, GT looks for the most recent 
		/// audit it created for this record.  If the timestamp of that audit is older than the load time of this record
		/// it allows the update, otherise an exception is thrown.
		/// </summary>
		// We ran into some timeing issue with the same processes doing an update twice.  The issue was due to NTP changing the dt.Now 
		// between the time the audit was written and the time we get SeeverDate.  So we now also use the audit id to help out
		public bool CheckConcurrency {
			get { return mConcurrencyCheck; }
			set {
				if (!AutomaticAuditHistory)
					throw new Exception("cant do concurrency check without audit");

				mConcurrencyCheck = true;
			}
		}

		public bool Recover {
			get { return (mAutoRecover == null); }
			set {
				if (value)
					mAutoRecover = GT_AUTORECOVER;
				else
					mAutoRecover = null;
			}
		}

		private long mID;
		public long ID {
			set { mID = value; }
			get { return mID; }
		}

		public bool TruncateStrings {
			set { mTruncateStrings = value; }
		}

		public string FullTablePath {
			get { return mFullTablePath; }
		}

		public string TableName {
			get { return mTableName; }
		}
		/// <summary>
		/// Allows an inheriting class to define its table name in a method rather than as a constructor argument.
		/// The default name is the same as the class name.
		/// </summary>
		/// <remarks>
		/// If you do not supply a "TableName" parameter to the constructor of a GT class, this property is used.
		/// If you do not override this property, the table name will be the same as the name of the class.
		/// </remarks>
		protected virtual string DefaultTableName {
			get { return this.GetType().Name; }
		}

		public TDatabase Database {
			get { return mSqlUtil.Database; }
		}
		/// <summary>
		/// Allows an inheriting class to define its database in a method rather than as a constructor argument.
		/// The default database if this is not overridden is Client.
		/// </summary>
		protected virtual TDatabase DefaultDatabase {
			get { return TDatabase.Client; }
		}

		/// <summary> 
		/// Indexer with a passthrough to GetFieldValue/SetFieldValue. 
		/// </summary>
		public virtual object this[string FieldName] {
			get { return GetFieldValue(FieldName); }
			set { SetFieldValue(FieldName, value); }
		}

		public virtual object GetFieldValue(string f) {
			GenericField gf = (GenericField)mTableDef[f];
			if (gf == null) {
				ThrowFieldException(f); //does not always exception out
				if (mData.ContainsKey(f))
					return mData[f];

				return null;
			}

			object ret = mData[f];
			if (ret == System.DBNull.Value || ret == null) {
				int type = gf.mType;
				return mTypedNullObject[type];
			}

			return ret;
		}

		public virtual void SetFieldValue(string FieldName, object val) {
			GenericField gf = (GenericField)mTableDef[FieldName];
			if (gf == null) {
				ThrowFieldException(FieldName);
				//we get here only if exception are ignored 
				// so we set it - without type checking - cause we have no type info
				//we also dont save it in change list cause it not a real field
				mData[FieldName] = val;
				return;
			}

			//we get here only for fields that are in the table

			if (val != null && (gf.mType == (int)MyDbTypes.String || gf.mType == (int)MyDbTypes.Char) &&
				((string)val).Length > gf.mLength) {
				if (!mTruncateStrings)
					throw new Exception("data for field " + FieldName + " too large(>" + gf.mLength + ") " + (string)val);

				val = ((string)val).Substring(0, gf.mLength);
			}

			object prev = mData[FieldName];
			if (CompareVals(prev, val) == 0)
				return;

			//avoid multple assigmnet erasing original value frm DB
			if (!mChangeList.ContainsKey(FieldName))
				mChangeList[FieldName] = prev; // save the old value

			//we may want to validate based on type
			mData[FieldName] = val;
		}

		private int CompareVals(object old, object newval) {
			if ((old == null || old == System.DBNull.Value) && newval == null)
				return 0;

			if (old != null && old.Equals(newval))
				return 0;

			return 1;
		}

		private static string ValidVar(string instr) {
			return instr.Replace(' ', '_');
		}

		public Dictionary<string, object> ChangeList {
			get { return mChangeList; }
		}

		#endregion

		#region Events
		public delegate void GenericTableEvent(GenericTable gt);
		public event GenericTableEvent Saved;
		#endregion

		#region constructors
		protected GenericTable() : this(TDatabase.Unknown) { }
		protected GenericTable(TDatabase db) : this("", db) { }
		protected GenericTable(TDatabase db, string IdColumn) : this("", db, IdColumn) { }

		public GenericTable(string TableName, TDatabase db) : this(TableName, db, null) { }

		public GenericTable(string TableName, TDatabase db, string IdColumn) {
			ConstructMe(TableName, db, IdColumn, null);
		}

		public GenericTable(SqlUtil sql, string TableName, string IdColumn) {
			//server and DB are ignored by construct me if sql is supplied
			ConstructMe(TableName, sql.Database, IdColumn, sql);
		}

		private void ConstructMe(string tableName, TDatabase db, string IdColumn, SqlUtil sql) {
			if (db == TDatabase.Unknown)
				db = this.DefaultDatabase;
			if (string.IsNullOrEmpty(tableName))
				tableName = this.DefaultTableName; // allows a subclass to define a tablename by method rather than passing in the constructor.

			if (IdColumn == null)
				IdColumn = "ID";

			mIdColumn = IdColumn;

			if (sql != null)
				mSqlUtil = sql;
			else
				mSqlUtil = SqlUtil.Get(db);

			if (tableName.StartsWith("["))
				mTableName = tableName;
			else
				mTableName = "[" + tableName + "]";

			mFullTablePath = mSqlUtil.FullTablePath(tableName);
			mTableDef = GetTableDef();
			mChangeList = new Dictionary<string, object>();

			if (mTableDef == null) {
				Exception x = new Exception("no ID field");
				throw x;
			}

			if (mIdColumn != (string)mIdColumnHash[mFullTablePath]) {
				Exception x = new Exception("cant redefined id column");
				throw x;
			}

			mHasIdentity = (bool)mHasIdentityCache[mFullTablePath];

			mData = new Dictionary<string, object>(mTableDef.Count + 3);
			//init all fields to null
			foreach (string f in mTableDef.Keys)
				mData[f] = System.DBNull.Value;

			mNewRecord = true;
		}
		#endregion

		#region public methods for IO
		public GenericField GetFieldInfo(string FieldName) {
			return (GenericField)mTableDef[FieldName];
		}


		public virtual int SqlRecoverCB() {
			this.CreateRecord(true);
			return 1;
		}

		public override string ToString() {
			string str = "";

			IEnumerator<KeyValuePair<string, object>> myEnum;
			SortedList<string, object> s = new SortedList<string, object>(mData);
			myEnum = s.GetEnumerator();

			str = mIdColumn + ":" + mID.ToString() +
				"\nTable:" + mFullTablePath +
				"\nContent\n\n";

			while (myEnum.MoveNext()) {
				str += myEnum.Current.Key.ToString() + ":" + DataUtils.BlankIfNull(myEnum.Current.Value).ToString() + "\n";
			}

			return str;
		}

		public void Clear() {
			mID = 0;
			mChangeList.Clear();
			mData.Clear();
			mNewRecord = true;
			mDataRow = null;
			mArchivedRecord = null;
			mArchivedColumn = null;
		}

		public int RemoveArchive() {
			if (this.ID < 1)
				return 0;

			int ctr = 0;
			if (mArchivedRecord != null && mArchivedRecord.Data != null) {
				//the trick here is to write the record out	- i guesst this could be moved inline
				//with the regular update logic so we could update an archoved record
				bool SaveHasIdent = mHasIdentity;
				ArchivedData da = mArchivedRecord;
				try {
					mArchivedRecord = null;
					mHasIdentity = false;
					//this is a very nasty hack - cause the write stmt will be messed up a bit
					mSqlUtil.ExecuteNoResultSetSQLQuery("set IDENTITY_INSERT " + mTableName + " on");
					this.CreateRecord(true);
					mSqlUtil.ExecuteNoResultSetSQLQuery("set IDENTITY_INSERT " + mTableName + " off");
					da.mArchiveControl.ArchiveMethod.Delete(this.ID);
					ctr++;
				} finally {
					mHasIdentity = SaveHasIdent;

				}
			}

			if (mArchivedColumn != null && mArchivedColumn.Data != null) {
				//an update deletes the archive - but we need to force the update by assigning
				//one value to itself to the changecount > 0
				Dictionary<string, GenericField>.Enumerator e = mTableDef.GetEnumerator();
				e.MoveNext();
				string FirstField = (string)e.Current.Key;

				object o = this.GetFieldValue(FirstField);
				if (o.ToString() == "g")
					this.SetFieldValue(FirstField, "a");
				else
					this.SetFieldValue(FirstField, "g");

				//set it back to it orig value
				this.SetFieldValue(FirstField, o);
				this.UpdateRecord();
				ctr++;
			}

			return ctr;

		}

		/// <summary>
		/// Reads the record with a readlock.  Note that it is the developer's responsibility to create transaction boundaries.
		/// </summary>
		/// <param name="id"></param>
		/// <param name="WithReadLock"></param>
		/// <returns>1 if successful, 0 if ID not found.</returns>
		public int ReadRecord(long id, bool WithRowLock) {
			// note that we use the module-level variable so that we don't have to change the signature for the virtual method ReadRecord.
			mUseRowLock = WithRowLock;
			try {
				return ReadRecord(id);
			} finally {
				mUseRowLock = false;
			}
		}

		private bool mUseRowLock = false;

		public virtual int ReadRecord(long id) {
			mChangeList.Clear();
			SqlParameterCollection oParams = (new SqlCommand()).Parameters;
			oParams.AddWithValue("@ID", id);

			string LockHint = mUseRowLock ? " with (UPDLOCK)" : "";
			string ReadStmt = string.Format("select * from {0}{1} where [{2}]=@ID", mTableName, LockHint, mIdColumn);

			DataRow r;
			//we could of checked the archive before the read - however we want the output of
			//of the archived read to be a data row and there was no good way to serialize a 
			//data row.  So we do the read up front and if there is no data retunred we can
			//build the data row from the Hash.  We need the  DataTable with no rows in order to
			//insure all the DB info is correct (like column defs)
			//another advatage to reading first is record archive IDs need not be contigious now
			DataTable oTable = mSqlUtil.ExecuteSingleResultSetSQLQuery(ReadStmt, oParams);
			if (oTable.Rows.Count == 0) {
				mArchivedRecord = ArchivedData.ReadRecord(mFullTablePath, id);
				if (mArchivedRecord != null) {
					r = oTable.NewRow();

					Dictionary<string, object> h = mArchivedRecord.Data;
					object v;
					foreach (DataColumn c in oTable.Columns) {
						v = h[c.ColumnName];
						if (v == null)
							v = System.DBNull.Value;

						r[c.ColumnName] = v;
					}

					r[this.IDColumnName] = id;
					oTable.Rows.Add(r);
				} else {
					mID = id;
					return 0;
				}
			} else {
				r = oTable.Rows[0];
			}

			if (mData == null)
				mData = new Dictionary<string, object>(mTableDef.Count + 3);

			Load(r, true);

			return 1;
		}


		/// <summary>
		/// Subclass can override this with any fancy select bsed on any data.  
		/// </summary>
		/// <returns>id of record if it exists or 0</returns>
		protected virtual long ExistingRecordID() {
			if (mNewRecord)
				return 0;

			return mID;
		}

		/// <summary>
		/// Allows class to pre-process data before it is persisted.
		/// </summary>
		public virtual void PreWrite() {
		}

		public virtual long SaveRecord() {
			//if we already know this record exists - just update it.  
			//otherwise a call to ExistingRecordID could do extra selects for no reason
			if (!mNewRecord) {
				UpdateRecord();
				return mID;
			}

			//if we are not sure, then check with overridble method.  
			long id = ExistingRecordID();
			if (id < 1) {
				return ProtectedCreateRecord(false);
			} else {
				mID = id; //this may have been set by subclass
				mNewRecord = false;
				UpdateRecord();
				return mID;
			}
		}

		/// <summary>
		/// Inserts a new record into the database.
		/// </summary>
		/// <returns>ID of written record, or 0 if a recoverable error occurred and AutoRecover is on.</returns>
		public long CreateRecord() {
			return CreateRecord(false);
		}

		/// <summary>
		/// Inserts a new record into the database.
		/// </summary>
		/// <param name="force">Force a save even if no fields have changed.</param>
		/// <returns>ID of written record, or 0 if a recoverable error occurred and AutoRecover is on.</returns>
		public long CreateRecord(bool force) {
			if (!AllowDirectCreate)
				throw new Exception("may not do direct writes on table " + mTableName);

			return ProtectedCreateRecord(force);
		}

		/// <summary>
		/// protected so classes that dont want to allow application to use CreateRecord can set 
		/// the virtual property AllowDirectCreate to false
		/// While they still can do
		/// custom processing in their ovrrides of the protected method
		/// </summary>
		/// <param name="force"></param>
		/// <returns></returns>
		protected virtual long ProtectedCreateRecord(bool force) {
			if (mID < 1 && !mHasIdentity) {
				Exception x = new Exception("must set ID");
				throw x;
			}

			this.PreWrite();

			SqlParameterCollection oParams = CreateSqlParams(true);

			if (mChangeList.Count == 0 && !force)
				return 0;

			if (mWriteStmt == null) {
				mWriteStmt = "insert into " + mTableName + " (";

				string InsertFields = "";
				string InsertValues = "";
				foreach (string fname in mTableDef.Keys) {
					if (!((GenericField)mTableDef[fname]).mReadOnly) {
						InsertFields += "[" + fname + "],";
						InsertValues += "@" + ValidVar(fname) + ",";
					}
				}
				mWriteStmt += InsertFields;

				if (!mHasIdentity)
					mWriteStmt += "[" + mIdColumn + "]";
				else
					mWriteStmt = mWriteStmt.TrimEnd(",".ToCharArray());

				mWriteStmt += ") Values (" + InsertValues;

				if (!mHasIdentity)
					mWriteStmt += "@ID";
				else
					mWriteStmt = mWriteStmt.TrimEnd(",".ToCharArray());

				mWriteStmt += ")";
				//has SQL return the identity

				if (mHasIdentity) {
					mWriteStmt += "; select @@IDENTITY as ID";
				}
			}

			bool Recovered = false;
			try {
				if (mHasIdentity) {
					DataTable oTable = mSqlUtil.ExecuteSingleResultSetSQLQuery(mWriteStmt, oParams);
					if (oTable.Rows.Count == 0)
						return -1;

					mID = DataUtils.LongZeroIfNull(oTable.Rows[0]["ID"]);
				} else {
					oParams.AddWithValue("@ID", mID);
					mSqlUtil.ExecuteNoResultSetSQLQuery(mWriteStmt, oParams);
				}
			} catch (SqlException e) {
				if (SqlUtil.isTimeRelatedError(e.Message) && this.AutoRecover != null) {
					SqlRecover.SaveForRecovery(this, this.AutoRecover);
					Recovered = true;
					mID = 0;
				} else
					throw new Exception("Can't AutoRecover - " + e.Message, e);
			}

			if (AutomaticAuditHistory) {
				AuditFields af = new AuditFields();
				af.Add("create", null, null);
				CreateAudit(-1, af);
			}

			mNewRecord = false;
			mChangeList.Clear();
			if (!Recovered)
				RaiseSavedEvent();

			return mID;
		}

		public void UpdateRecord() {
			UpdateRecord(false);
		}

		public virtual void UpdateRecord(bool IncludeUnchangedFields) {
			if (mID < 0) {
				Exception x = new Exception("must set ID");
				throw x;
			}

			this.PreWrite();

			//even if its in the chnage list - it could be the app just did a few assigment stmts 
			//and in the end the data value is still the same
			string[] keys = new string[mChangeList.Count];
			mChangeList.Keys.CopyTo(keys, 0); //cant modify an enumeration in middle of the loop - so we copy out the keys
			foreach (string k in keys) {
				if (CompareVals(mChangeList[k], mData[k]) == 0)
					mChangeList.Remove(k);
			}

			if (mChangeList.Count == 0)
				return;

			ConcurrencyViolationCheck(true);

			//for an archive update we delete the archive and assume the data will be in the primary table.  
			//that will only happen if the fields changed - which they may have not.  One solution is 
			//when we merge in the archived columsn to treat that as a change.  But then that would mean a save 
			//would write even when nothing really chnaged.  Or we just turn on IncludeUnchnagedFIleds
			//on an update that had an archive.
			// just found out (5/2008) that you cant update a clustered index and a text field in the same stmt
			//so we will need to break this down into two updates.  One for the archived fields and one for normail update.
			//this only solves the problem for archived data.   
			if (mArchivedColumn != null) {
				//create a temp chnage list that only has the archived colunms
				//do that update and the com back and do the other update
				Dictionary<string, object> ArchivedChangedList = new Dictionary<string, object>();
				foreach (string k in mArchivedColumn.Data.Keys) {
					ArchivedChangedList[k] = mData[k];
					mChangeList.Remove(k);
				}

				ApplyUpdate(IncludeUnchangedFields, ArchivedChangedList);
			}

			//if we applid the update to the arcive, it could be there is no more work to do
			if (mChangeList.Count > 0) {
				ApplyUpdate(IncludeUnchangedFields, mChangeList);
			}

			DateTimeUtility.ServerDate(true);
			// insert an audit record if necessary
			if (AutomaticAuditHistory) {
				AuditFields af = new AuditFields();
				foreach (string fname in mChangeList.Keys) {
					if (mTableDef.ContainsKey(fname)) {
						af.Add(fname, mChangeList[fname], mData[fname]);
					}
				}

				CreateAudit(-2, af);
			}

			//concurency time get updated 
			mLoadTime = DateTimeUtility.ServerDate();

			//if there was a column archive on this - delete the archive record.  This allows 
			// the app to update the record and not have
			//the update lost by a merge of archived ata that takes place on load.
			//this gets done only after a successful update took place
			if (mArchivedColumn != null)
				mArchivedColumn.Delete();

			mChangeList.Clear();
			RaiseSavedEvent();
		}

		private void ApplyUpdate(bool IncludeUnchangedFields, Dictionary<string, object> FieldChangeList) {

			SqlParameterCollection oParams = CreateSqlParams(IncludeUnchangedFields, FieldChangeList);

			string UpdateStmt = "update " + mTableName + " set ";
			foreach (string fname in mTableDef.Keys) {
				if (!mTableDef[fname].mReadOnly && (IncludeUnchangedFields || FieldChangeList.ContainsKey(fname))) {
					UpdateStmt += "[" + fname + "]=@" + ValidVar(fname) + ",";
				}
			}

			UpdateStmt = UpdateStmt.TrimEnd(",".ToCharArray()) + " where [" + mIdColumn + "]=@ID";

			oParams.AddWithValue("@ID", mID);
			mSqlUtil.ExecuteNoResultSetSQLQuery(UpdateStmt, oParams);
		}

		private void RaiseSavedEvent() {
			if (Saved != null) {
				Saved(this);
			}
		}


		public virtual void DeleteRecord() {
			if (mID < 0) {
				Exception x = new Exception("must set ID");
				throw x;
			}

			if (mDeleteStmt == null)
				mDeleteStmt = "delete from " + mTableName + " where [" + mIdColumn + "]= @ID";

			SqlParameterCollection oParams = (new SqlCommand()).Parameters;
			oParams.AddWithValue("@ID", mID);

			mSqlUtil.ExecuteNoResultSetSQLQuery(mDeleteStmt, oParams);

			//remove from archve also
			if (mArchivedColumn != null)
				mArchivedColumn.Delete();

			if (mArchivedRecord != null)
				mArchivedRecord.Delete();

			mArchivedRecord = null;
			mArchivedColumn = null;
			mNewRecord = true;

			if (AutomaticAuditHistory) {
				AuditFields af = new AuditFields();
				af.Add("delete", null, null);
				CreateAudit(-3, af);
			}

		}

		public virtual void Load(DataRow r) {
			Load(r, true);
		}

		/// <summary>
		/// Shortcut to load data into gt from another database source, such as a select statement.
		/// Rows specified as encrypted in the table definition will be decrypted
		/// </summary>
		/// <param name="r">A row of data from a database</param>
		public void Load(DataRow r, bool UseArchive) {
			mDataRow = r;
			mID = DataUtils.LongZeroIfNull(r[mIdColumn]);
			mNewRecord = false;

			//get archived columns and merge in to data row
			if (UseArchive) {
				mArchivedColumn = ArchivedData.ReadColumns(mFullTablePath, this);
				if (mArchivedColumn != null) {
					foreach (KeyValuePair<string, object> kvp in mArchivedColumn.Data) {
						r[kvp.Key] = kvp.Value;
					}
				}
			} else
				mArchivedColumn = null;

			foreach (KeyValuePair<string, GenericField> kvp in mTableDef) {
				string fname = kvp.Key;
				GenericField gf = kvp.Value;
				if (!r.Table.Columns.Contains(fname) && this.IgnoreMissingFields) {
					mData[fname] = DBNull.Value;
				} else if (gf.mEncrypted == true && r[fname] != DBNull.Value) {
					this.DecryptCol(r[fname], gf, fname);
				} else {
					mData[fname] = r[fname];
				}
			}

			this.mChangeList.Clear();
			mLoadTime = DateTimeUtility.ServerDate();
		}

		private void EncryptCol(ref object FieldValue, GenericField gf, string FieldName) {
			switch (gf.mType) {
				case (int)MyDbTypes.String:
					FieldValue = EncryptionUtil.SimpleEncrypt((string)mData[FieldName], FieldName);
					break;
				case (int)MyDbTypes.Image:
					FieldValue = EncryptionUtil.SimpleEncrypt((byte[])mData[FieldName], FieldName);
					break;
			}
		}

		private void DecryptCol(object FieldValue, GenericField gf, string FieldName) {
			switch (gf.mType) {
				case (int)MyDbTypes.String:
					mData[FieldName] = EncryptionUtil.SimpleDecrypt((string)FieldValue, FieldName);
					break;
				case (int)MyDbTypes.Image:
					mData[FieldName] = EncryptionUtil.SimpleDecrypt((byte[])FieldValue, FieldName);
					break;
			}
		}

		private void CreateAudit(int type, AuditFields af) {
			if (AdditionalAuditData != null) {
				string oldVal;
				string newVal;
				foreach (KeyValuePair<string, string[]> AuditEntry in AdditionalAuditData.UpdateList) {
					AuditFields.ExtractAuditValue(AuditEntry.Value, out oldVal, out newVal);
					af.Add((string)AuditEntry.Key, oldVal, newVal);
				}

				AdditionalAuditData = null;
			}

			string EventSrc = null;
			long EventID = 0;
			try {
				AuditLog al = new AuditLog((long)type, TableName + ":Audit", ID, EventSrc, EventID, af, AuditHistoryDatabase);

				//mLastAuditID is used for concurrency checking
				if (AuditHistoryDatabase == TDatabase.Unknown)
					mLastAuditID = al.Save(AuditLog.CompletionStatus.Success, Process.GetCurrentProcess().ProcessName, this.sqlUtil);
				else
					mLastAuditID = al.Save(AuditLog.CompletionStatus.Success, Process.GetCurrentProcess().ProcessName);
			} catch (Exception) {
				// don't crash the overall delete if the audit record failed to insert
				// TODO - log this error somewhere else
			}
		}

		/// <summary>
		/// return the audithistory for this item's ID.  AuditHistory is an array list of AuditLogs objects in time order
		/// </summary>
		/// <returns></returns>
		public List<AuditLog> GetAuditHistory() {
			string[] types = new string[] { this.TableName + ":Audit" };
			return AuditLog.GetAuditListByDataType(types, this.ID, false, true, this.sqlUtil);
		}


		/// <summary>
		/// returns true if DB record has been changed since the load.  If uses the audit history to detect change.  
		/// it will throw an exception if this is called and audit history is not enabled.
		/// </summary>
		/// <returns></returns>
		public bool HasDBRecordChangedSinceLoad() {
			if (!AutomaticAuditHistory)
				throw new Exception("audit history must be on for this feature to work");

			return ConcurrencyViolationCheck(false);
		}

		private bool ConcurrencyViolationCheck(bool ThrowException) {
			if (!mConcurrencyCheck)
				return false;

			SqlUtil sql;
			if (AuditHistoryDatabase == TDatabase.Unknown)
				sql = this.sqlUtil;
			else
				sql = new SqlUtil(AuditHistoryDatabase);

			string q = "select top 1 ID, LogTime from AuditHeader where DataType = @type and DataId = @id order by id desc";
			SqlParameterCollection parms = (new SqlCommand()).Parameters;
			parms.AddWithValue("@id", this.ID);
			parms.AddWithValue("@type", TableName + ":Audit");
			DataTable dt = sql.ExecuteSingleResultSetSQLQuery(q, parms);
			if (dt.Rows.Count == 0)
				return false;

			object IdObject = dt.Rows[0][0];
			if (IdObject == null)
				return false;

			object TimeObject = dt.Rows[0][1];
			if (TimeObject == null)
				return false;

			//dateTime.Now can change underneath us via NTP, and we therefore do not want to rely on the timestamp.  So, if we have an Auditid,we use it
			bool err = false;
			if (mLastAuditID > 0) {
				if ((long)IdObject > mLastAuditID)
					err = true;
				else
					return false;
			}

			if (err || (DateTime)TimeObject > mLoadTime) {
				if (ThrowException)
					throw new Exception(string.Format("concurrency violation for table {0}:id {1}, LoadTime {2}, LastUpdateTime from audit {3}. AuditId {4}, Last AuditID from audit {5}",
						TableName, ID, mLoadTime, TimeObject, mLastAuditID, IdObject));

				return true;
			}

			return false;
		}
		#endregion

		#region private
		private SqlParameterCollection CreateSqlParams(bool IncludeUnchangedFields) {
			return CreateSqlParams(IncludeUnchangedFields, mChangeList);
		}

		private SqlParameterCollection CreateSqlParams(bool IncludeUnchangedFields, Dictionary<string, object> ChangeList) {
			SqlParameterCollection oParams = (new SqlCommand()).Parameters;

			int type;
			SqlDbType dbtype;
			object theValue;
			SqlParameter p;
			GenericField gf;
			String fname;

			if (this.ReadOnly)
				throw new Exception("cant not write to ReadOnly table");

			//we maybe allow this - but that would mean we would need to delete the item from the archive 
			// and do a write instead of an update to the real table.
			if (mArchivedRecord != null)
				throw new Exception("cant write archived record " + mArchivedRecord.mArchiveControl.TablePath);


			foreach (KeyValuePair<string, GenericField> kvp in mTableDef) {
				theValue = null;
				fname = kvp.Key;
				gf = kvp.Value;
				if (gf.mReadOnly)
					continue;

				if (!IncludeUnchangedFields && !ChangeList.ContainsKey(fname))
					continue;

				if (gf.mEncrypted && mData[fname] != null && mData[fname] != DBNull.Value) {
					this.EncryptCol(ref theValue, gf, fname);
				} else {
					theValue = mData[fname];
				}

				//always type it
				type = gf.mType;
				dbtype = mSqlDbType[type];
				p = oParams.Add("@" + ValidVar(fname), dbtype);
				if (theValue == null || theValue == System.DBNull.Value)
					p.Value = System.DBNull.Value;
				else if ((gf.mType == (int)MyDbTypes.Date || gf.mType == (int)MyDbTypes.smallDate) && DataUtils.ToDate(theValue).Equals(DateTime.MinValue))
					p.Value = System.DBNull.Value;
				else
					p.Value = theValue;
			}

			return oParams;
		}

		private void ThrowFieldException(string f) {
			if (IgnoreUnknownFields)
				return;

			Exception x = new Exception("trying to access unknown field " + f +
				" for table " + mFullTablePath);
			throw x;
		}

		private Dictionary<string, GenericField> GetTableDef() {
			if (mAllTableDefs == null)
				DoOnetimeInits();

			if (mAllTableDefs.ContainsKey(mFullTablePath))
				return mAllTableDefs[mFullTablePath];

			bool HasIdField = false;
			//remove brakets
			string tableName = mTableName.Substring(1, mTableName.Length - 2);
			string sel = "select a.name,a.length,a.xtype,a.colstat, a.iscomputed from syscolumns as a " +
				"inner join sysobjects as b on a.id = b.id " +
				"where b.name = '" + tableName + "'";

			Dictionary<string, GenericField> def = new Dictionary<string, GenericField>();

			mHasIdentityCache[mFullTablePath] = false;

			DataTable oResult = mSqlUtil.ExecuteSingleResultSetSQLQuery(sel);
			Type DbtypeEnum = typeof(MyDbTypes);

			foreach (DataRow oRow in oResult.Rows) {
				if (oRow["name"].Equals(mIdColumn)) {
					mIdColumnHash[mFullTablePath] = mIdColumn;
					HasIdField = true;
					if ((short)oRow["colstat"] == 1 || (short)oRow["colstat"] == 9)
						mHasIdentityCache[mFullTablePath] = true;
				} else {
					GenericField f = new GenericField();
					f.mType = (byte)oRow["xtype"];
					f.mLength = (short)oRow["length"];
					if (!Enum.IsDefined(DbtypeEnum, f.mType))
						throw new Exception(mFullTablePath + ":undefined DBtype " + f.mType
								+ " for field " + (string)oRow["name"]);

					f.mEncrypted = DBMetaData.isColumnEncrypted(mFullTablePath, (string)oRow["name"]);
					if (f.mEncrypted && (f.mType != (int)MyDbTypes.String && f.mType != (int)MyDbTypes.Image)) {
						throw new Exception(mFullTablePath + ":Column " + (string)oRow["name"] + " is not type varchar and cannot be set as an encrypted column.");
					}

					def[(string)oRow["name"]] = f;
					if ((int)oRow["iscomputed"] == 1) {
						f.mReadOnly = true;
					}

				}
			}

			if (!HasIdField)
				return null;


			mAllTableDefs[mFullTablePath] = def;

			return def;
		}

		private void DoOnetimeInits() {
			mAllTableDefs = new Dictionary<string, Dictionary<string, GenericField>>();
			mTypedNullObject = new object[256];

			int i;
			for (i = 0; i < 256; i++)
				mTypedNullObject[i] = null;

			mTypedNullObject[(int)MyDbTypes.Byte] = (byte)0;
			mTypedNullObject[(int)MyDbTypes.Bool] = false;
			mTypedNullObject[(int)MyDbTypes.Short] = (short)0;
			mTypedNullObject[(int)MyDbTypes.Integer] = (int)0;
			mTypedNullObject[(int)MyDbTypes.Money] = (decimal)0;
			mTypedNullObject[(int)MyDbTypes.Float] = (float)0;
			mTypedNullObject[(int)MyDbTypes.Long] = (long)0;
			mTypedNullObject[(int)MyDbTypes.smallDate] = System.DateTime.MinValue;
			mTypedNullObject[(int)MyDbTypes.Date] = System.DateTime.MinValue;
			mTypedNullObject[(int)MyDbTypes.String] = "";
			mTypedNullObject[(int)MyDbTypes.Text] = "";
			mTypedNullObject[(int)MyDbTypes.Char] = "";
			mTypedNullObject[(int)MyDbTypes.Image] = (string)null;
			mTypedNullObject[(int)MyDbTypes.Double] = (double)0;

			mSqlDbType = new SqlDbType[256];
			for (i = 0; i < 256; i++)
				mSqlDbType[i] = SqlDbType.NVarChar;

			mSqlDbType[(int)MyDbTypes.Byte] = SqlDbType.TinyInt;
			mSqlDbType[(int)MyDbTypes.Bool] = SqlDbType.Bit;
			mSqlDbType[(int)MyDbTypes.Short] = SqlDbType.SmallInt;
			mSqlDbType[(int)MyDbTypes.Integer] = SqlDbType.Int;
			mSqlDbType[(int)MyDbTypes.Money] = SqlDbType.Money;
			mSqlDbType[(int)MyDbTypes.Float] = SqlDbType.Real;
			mSqlDbType[(int)MyDbTypes.Long] = SqlDbType.BigInt;
			mSqlDbType[(int)MyDbTypes.smallDate] = SqlDbType.DateTime;
			mSqlDbType[(int)MyDbTypes.Date] = SqlDbType.DateTime;
			mSqlDbType[(int)MyDbTypes.String] = SqlDbType.VarChar;
			mSqlDbType[(int)MyDbTypes.Char] = SqlDbType.Char;
			mSqlDbType[(int)MyDbTypes.Text] = SqlDbType.Text;
			mSqlDbType[(int)MyDbTypes.Image] = SqlDbType.Image;
			mSqlDbType[(int)MyDbTypes.Double] = SqlDbType.Float;
		}

		#endregion

		//get any record from a any database and returns ToString on it
		public static string AnyTableToString(string tablename, TDatabase db, long uid) {
			string DispString;
			if ((tablename == null)) {
				DispString = "unknown data type";
			} else {
				try {
					GenericTable gt = new GenericTable(tablename, db);
					gt.ReadRecord(uid);
					DispString = gt.ToString();
				} catch (Exception x) {
					DispString = "Error getting record " + x.Message;
				}
			}

			return DispString;
		}

		public static GenericTable GetFromHistoryOrCurrent(string TableName, TDatabase db, long id) {
			GenericTable gt = new GenericTable(TableName, db);
			if (gt.ReadRecord(id) > 0)
				return gt;

			gt = new GenericTable("History" + TableName, db);
			if (gt.ReadRecord(id) > 0)
				return gt;

			return null;
		}

	}
}
