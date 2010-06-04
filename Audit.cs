using System;
using System.Collections.Generic;
using System.Reflection;
using System.Diagnostics;
using System.Data;
using System.Data.SqlClient;

namespace SQLinqenlot {
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	[Serializable()]
	public class AuditFields {
		private Dictionary<string, string[]> mUpdateList;

		public AuditFields() {
			mUpdateList = new Dictionary<string, string[]>();
		}


		/// <summary>
		/// 
		/// </summary>
		/// <param name="obj"></param>
		/// <param name="o"></param>
		/// <param name="n"></param>
		public static void ExtractAuditValue(object AuditValues, out string o, out string n) {
			string[] vals = (string[])AuditValues;
			o = vals[0];
			n = vals[1];

		}

		public override string ToString() {
			return ToString("\t", "\n");
		}

		public string ToString(string FieldSep, string LineSep) {
			string o;
			string n;
			string str = "";

			foreach (string f in mUpdateList.Keys) {
				GetAuditValue(f, out o, out n);
				str += f + FieldSep + o + FieldSep + n + LineSep;
			}

			return str;
		}

		public Dictionary<string, string[]> UpdateList {
			get { return mUpdateList; }
		}

		public bool GetAuditValue(string f, out string o, out string n) {
			string[] vals;
			bool found;

			try {
				vals = mUpdateList[f];
				o = vals[0];
				n = vals[1];
				found = true;
			} catch {
				found = false;
				o = null;
				n = null;
			}

			return found;
		}

		public bool Exist(string f) {
			if (mUpdateList == null)
				return false;

			return mUpdateList.ContainsKey(f);
		}

		//copies in another audit list to this one 
		public void Add(String KeyPrefix, AuditFields source) {
			string prefix = KeyPrefix + ".";

			foreach (string k in source.mUpdateList.Keys) {
				mUpdateList[prefix + k] = source.mUpdateList[k];
			}
		}

		public void Add(string f, string o, string n) {
			string[] vals = new string[2];
			vals[0] = o;
			vals[1] = n;

			mUpdateList[f] = vals;
		}

		public void Add(string f, object o, object n) {
			string[] vals = new string[2];
			if (o == null)
				vals[0] = "null";
			else
				vals[0] = o.ToString();

			if (n == null)
				vals[0] = "null";
			else
				vals[1] = n.ToString();

			mUpdateList[f] = vals;
		}

		public void Add(DataRow r) {
			if (r["Field"] == System.DBNull.Value)
				return;

			string[] vals = new string[2];
			vals[0] = (string)DataUtils.BlankIfNull(r["OldValue"]);
			vals[1] = (string)DataUtils.BlankIfNull(r["NewValue"]);

			mUpdateList[r["Field"].ToString()] = vals;
		}

		public Dictionary<string, string[]>.Enumerator GetEnumerator() {
			return mUpdateList.GetEnumerator();
		}

	}


	[Serializable()]
	public class AuditLog {

		public enum CompletionStatus : short {
			Success = 1,
			Error = -1,
			Ignore = 0,
			Exception = -2,
			PermanentError = -3,
		}

		public static bool IsError(CompletionStatus s) {
			short i = (short)s;
			if (s < 0)
				return true;

			return false;
		}

		public enum ObjectType : byte {
			Case = 1,
			Account = 2,
		}

		private long mCorrespondenceID;
		private string mDataType;
		private long mDataID;
		private AuditFields mFieldChanges;
		private string mEventDescription;
		private long mEventID;
		private long mID;
		private DateTime mLogTime;
		private string mCompletionMsg;
		private CompletionStatus mCompletionStatus;
		private TDatabase mDatabase;
		private bool mRecover;

		#region constructor
		/// <summary>
		/// create an auditlog from an sql row
		/// Appears to be a row from AuditHeader table
		/// </summary>
		/// <param name="r"></param>
		/// <param name="FillDetails">if set, will get all detail records for this 'ID' and fill in FieldChanges</param>

		public AuditLog(DataRow r, bool FillDetails) : this(r, FillDetails, TDatabase.Client) { }

		public AuditLog(DataRow r, bool FillDetails, TDatabase Database) {
			mID = DataUtils.LongZeroIfNull(r["ID"]);

			mDataType = (string)DataUtils.BlankIfNull(r["DataType"]);
			mEventDescription = (string)DataUtils.BlankIfNull(r["EventDescription"]);
			mCompletionMsg = (string)DataUtils.BlankIfNull(r["CompletionMsg"]);

			mCorrespondenceID = DataUtils.LongZeroIfNull(r["CorrespondenceID"]);
			mDataID = DataUtils.LongZeroIfNull(r["DataID"]);
			mCompletionStatus = (AuditLog.CompletionStatus)DataUtils.LongZeroIfNull(r["CompletionStatus"]);
			mLogTime = (DateTime)r["LogTime"];
			mDatabase = Database;
			mFieldChanges = new AuditFields();
			if (FillDetails) {
				SqlParameterCollection oParameters = new SqlCommand().Parameters;

				oParameters.AddWithValue("@ID", mID);

				string query = "select * from AuditDetail where AuditID =  @ID";

				DataTable oTable = SqlUtil.Get(mDatabase).ExecuteSingleResultSetSQLQuery(query, oParameters);
				if (oTable.Rows.Count == 0)
					return;

				foreach (DataRow r2 in oTable.Rows) {
					mFieldChanges.Add(r2);
				}
			}
		}

		public AuditLog(long correspondence_id, string datatype, long dataid, string EventDescript, long eid,
			AuditFields changeList)
			: this(correspondence_id, datatype, dataid, EventDescript, eid, changeList, TDatabase.Client) { }

		public AuditLog(long correspondence_id, string datatype, long dataid, string EventDescript, long eid,
			AuditFields changeList, TDatabase Database) {
			mDataType = datatype;
			mCorrespondenceID = correspondence_id;
			mDataID = dataid;
			mFieldChanges = changeList;
			mEventID = eid;
			mEventDescription = EventDescript;
			mDatabase = Database;
		}

		#endregion

		#region accessor

		public bool Recover {
			get { return mRecover; }
			set { mRecover = value; }
		}

		public AuditFields Fields {
			get { return mFieldChanges; }
		}

		public DateTime LogTime {
			get { return mLogTime; }
		}

		public string TransType {
			get { return mDataType; }
			set { mDataType = value; }
		}

		public long ID {
			get { return mID; }
		}

		public long EventID {
			get { return mEventID; }
			set { mEventID = value; }
		}

		public string EventDescription {
			get { return mEventDescription; }
			set { mEventDescription = value; }
		}

		public string CompletionMsg {
			get { return mCompletionMsg; }
			//set {mCompletionMsg = value;}
		}

		public AuditLog.CompletionStatus Status {
			get { return mCompletionStatus; }
			//set {mCompletionMsg = value;}
		}

		public long CorrespondenceID {
			get { return mCorrespondenceID; }
			set { mCorrespondenceID = value; }
		}

		public long DataID {
			get { return mDataID; }
			set { mDataID = value; }
		}

		//Why do we need this both as DataType and TransType?
		public string DataType {
			get { return mDataType; }
			set { mDataType = value; }
		}

		#endregion

		#region public methods
		public void AddException(Exception x) {
			if (mFieldChanges == null)
				mFieldChanges = new AuditFields();

			AuditLog.AddExceptionInfo(x, null, mFieldChanges);
		}

		public int SqlRecoverCB() {
			this.Save(mCompletionStatus, mCompletionMsg);

			return 1;
		}

		public long Save(CompletionStatus status, string CompletionMsg) {
			return Save(status, CompletionMsg, SqlUtil.Get(mDatabase));
		}

		//Seperate Connection is not used?
		public long Save(CompletionStatus status, string CompletionMsg, bool SeperateConnection) {
			SqlUtil sql = new SqlUtil(mDatabase);
			sql.BypassTransaction = true;
			return Save(status, CompletionMsg, sql);
		}

		/// <summary>
		/// you can supply your own SQL object and force the audit to go to that database.  Used by GT
		/// </summary>
		/// <param name="status"></param>
		/// <param name="CompletionMsg"></param>
		/// <param name="SQL"></param>
		/// <returns></returns>
		public long Save(CompletionStatus status, string CompletionMsg, SqlUtil SQL) {
			//save it - just in case of recovery
			mCompletionStatus = status;
			mCompletionMsg = CompletionMsg;

			string sql;


			SqlParameterCollection oParameters = new SqlCommand().Parameters;
			long ID = -1;

			oParameters.AddWithValue("@iDataType", DataUtils.DBNullIfNull(mDataType));
			oParameters.AddWithValue("@iDataID", DataUtils.DBNullIfNull(mDataID));
			oParameters.AddWithValue("@iCorrespondenceID", DataUtils.DBNullIfNull(mCorrespondenceID));
			oParameters.AddWithValue("@iEventID", DataUtils.DBNullIfNull(mEventID));
			oParameters.AddWithValue("@iEventDescription", DataUtils.DBNullIfNull(mEventDescription));
			oParameters.AddWithValue("@iCompletionMsg", DataUtils.DBNullIfNull(CompletionMsg));
			oParameters.AddWithValue("@iCompletionStatus", DataUtils.DBNullIfNull(status));
			if (mLogTime == DateTime.MinValue)
				mLogTime = DateTimeUtility.ServerDateNoRefresh();

			oParameters.AddWithValue("@iTime", mLogTime);

			sql = "set ansi_warnings off;INSERT INTO AuditHeader (DataType,DataID,CorrespondenceID,EventID,EventDescription,CompletionMsg,CompletionStatus,LogTime)"
				+ " VALUES (@iDataType,@iDataID,@iCorrespondenceID,@iEventID,@iEventDescription,@iCompletionMsg,@iCompletionStatus,@iTime)";
			sql += "; select @@IDENTITY as ID";

			DataTable oTable;
			try {
				oTable = SQL.ExecuteSingleResultSetSQLQuery(sql, oParameters);
			} catch (Exception x) {
				if (mRecover && SqlRecover.SaveForRecovery(x, true, this, "auditheader"))
					return 0;
				else
					throw new Exception("sql error", x);
			}

			if (oTable.Rows.Count == 0)
				return -1;

			ID = DataUtils.LongZeroIfNull(oTable.Rows[0]["ID"]);
			if (ID < 1)
				return -1;

			if (mFieldChanges != null) {
				string o;
				string n;
				oParameters = new SqlCommand().Parameters;
				foreach (string field in mFieldChanges.UpdateList.Keys) {
					mFieldChanges.GetAuditValue(field, out o, out n);

					oParameters.AddWithValue("@iAuditID", ID);
					oParameters.AddWithValue("@iField", DataUtils.DBNullIfNull(field));
					oParameters.AddWithValue("@iOldVal", DataUtils.DBNullIfNull(o));
					oParameters.AddWithValue("@iNewVal", DataUtils.DBNullIfNull(n));

					sql = "set ansi_warnings off;INSERT INTO AuditDetail(AuditID,Field, OldValue,NewValue) " +
						 " VALUES (@iAuditID,@iField, @iOldVal,@iNewVal);";

					SQL.ExecuteNoResultSetSQLQuery(sql, oParameters);
					oParameters.Clear();
				}
			}

			return ID;
		}


		public void FieldUpdate(string f, string o, string n) {
			if (mFieldChanges == null)
				mFieldChanges = new AuditFields();

			mFieldChanges.Add(f, o, n);
		}


		public void DeleteAudit() {
			SqlParameterCollection parms = new SqlCommand().Parameters;
			parms.AddWithValue("@ID", this.ID);

			SqlUtil sql = new SqlUtil(mDatabase);
			sql.ExecuteNoResultSetSQLQuery("delete from auditdetail where auditid = @ID", parms);

			parms = new SqlCommand().Parameters;
			parms.AddWithValue("@ID", this.ID);
			sql.ExecuteNoResultSetSQLQuery("delete from auditheader where id = @ID", parms);
		}

		#endregion

		#region public statics
		public static bool bRecursiveCall = false;
		/// <summary>
		/// save audit in database.Audit
		/// </summary>
		/// <param name="x"></param>
		/// <param name="msg"></param>
		/// <param name="ErrorOriginatingMethod"></param>
		public static void AuditException(Exception x, string msg, MethodBase ErrorOriginatingMethod) {
			AuditException(TDatabase.Client, x, msg, ErrorOriginatingMethod);
		}

		public static void AuditException(TDatabase db, Exception x, string msg, MethodBase ErrorOriginatingMethod) {
			try {
				if (bRecursiveCall)
					return;

				bRecursiveCall = true;
				AuditFields changelist = new AuditFields();
				AddExceptionInfo(x, ErrorOriginatingMethod, changelist);

				string EventSrc = Environment.MachineName;
				long EventID = 0;

				AuditLog l = new AuditLog(0, "Exception", 0, EventSrc, EventID, changelist, db);
				l.Recover = true;
				l.Save(AuditLog.CompletionStatus.Exception, msg, true);
			} catch { } finally {
				bRecursiveCall = false;
			}
		}

		private static void AddExceptionInfo(Exception x, MethodBase ErrorOriginatingMethod, AuditFields changelist) {
			Process oProcess = Process.GetCurrentProcess();

			changelist.Add("Exception details", x.Message, x.ToString());
			changelist.Add("Program details", oProcess.MainModule.FileName, DBLocator.ActiveClientName);
			changelist.Add("caller details", ErrorOriginatingMethod == null ? null : ErrorOriginatingMethod.ReflectedType.FullName,
				ErrorOriginatingMethod == null ? null : ErrorOriginatingMethod.Name);

			if (x.InnerException != null)
				changelist.Add("Inner Exception details", x.InnerException.Message, x.InnerException.ToString());

		}


		/// <summary>
		/// get the audit list for datatypes supplied and adds in the audits for any correspondeces 
		/// </summary>
		/// <param name="type"></param>
		/// <param name="DataId"></param>
		/// <returns></returns>
		public static List<AuditLog> GetAuditListByDataType(string[] type, long DataId) {
			return GetAuditListByDataType(type, DataId, true, false);
		}

		public static List<AuditLog> GetAuditListByDataType(string[] type, long DataId, bool IncludeCorrespondenceLog, bool NoErrors) {
			return GetAuditListByDataType(type, DataId, IncludeCorrespondenceLog, NoErrors, TDatabase.Client);
		}

		/// <summary>
		/// get the audit list for datatypes supplied and optionaly adds in the audits for any correspondeces 
		/// </summary>
		/// <param name="type"></param>
		/// <param name="DataId"></param>
		/// <param name="IncludeCorrespondenceLog"></param>
		/// <param name="NoErrors">do not include audit errors</param>
		/// <param name="SourceDB">The database that conatin the audit table you want to search since audit exists in almost every DB</param>
		/// <returns></returns>
		public static List<AuditLog> GetAuditListByDataType(string[] type, long DataId, bool IncludeCorrespondenceLog, bool NoErrors, TDatabase SourceDB) {
			return GetAuditListByDataType(type, DataId, IncludeCorrespondenceLog, NoErrors, SqlUtil.Get(SourceDB));
		}

		/// <summary>
		/// get the audit list for datatypes supplied and optionaly adds in the audits for any correspondeces 
		/// </summary>
		/// <param name="type"></param>
		/// <param name="DataId"></param>
		/// <param name="IncludeCorrespondenceLog"></param>
		/// <param name="NoErrors">do not include audit errors</param>
		/// <param name="sql">An sqlutil object of the database that conatin the audit table you want to search since audit exists in almost every DB</param>
		/// <returns></returns>
		public static List<AuditLog> GetAuditListByDataType(string[] type, long DataId, bool IncludeCorrespondenceLog, bool NoErrors, SqlUtil sql) {
			SqlParameterCollection oParameters = new SqlCommand().Parameters;

			oParameters.AddWithValue("@ID", DataId);
			string TypeList = String.Join("','", type);
			string query;
			string noError;
			if (NoErrors)
				noError = " and h1.completionstatus > -1 ";
			else
				noError = "";

			if (IncludeCorrespondenceLog) {
				query = "select h1.* into #t from auditheader h1  with (readpast)  " +
						"where h1.DataType in ('" + TypeList + "') and h1.DataId = @ID  " + noError +
				"select * from auditheader h1 with (readpast) " +
					"left join auditdetail d on h1.id = d.auditid " +
					"where h1.id in (select id from #t) " +
				"union " +
					"select h1.*, d.* from auditheader  h1 with (nolock) " +
						"left join auditdetail d with (nolock) on h1.id = d.auditid  " +
						"where  h1.correspondenceid in (select distinct correspondenceid from #t where correspondenceid > 0) " +
						"order by h1.id asc " +
				"drop table #t";
			} else {
				query = "select * from auditheader h1 with (readpast) " +
					"left join auditdetail d on h1.id = d.auditid " +
					"where h1.DataType in ('" + TypeList + "') and h1.DataId = @ID " + noError +
					"order by h1.id asc";
			}

			return GetAuditList(query, oParameters, sql, sql.Database);
		}

		/// <summary>
		/// get the audit list only for audit that contain the fields supplied in them.  
		/// </summary>
		/// <param name="type"></param>
		/// <param name="DataId"></param>
		/// <param name="FieldList"></param>
		/// <param name="NoErrors">do not include audit errors</param>
		/// <returns></returns>
		public static List<AuditLog> GetAuditListByField(string[] type, long DataId, string[] FieldList, bool NoErrors) {
			List<AuditLog> alist = GetAuditListByDataType(type, DataId, false, NoErrors);
			List<AuditLog> retlist = new List<AuditLog>();
			foreach (AuditLog al in alist) {
				foreach (string f in FieldList) {
					if (al.Fields.UpdateList.ContainsKey(f)) {
						retlist.Add(al);
						break;
					}
				}
			}

			return retlist;
		}

		public static List<AuditLog> GetAuditListByCorrespondence(long CorrespondenceID) {
			SqlParameterCollection oParameters = new SqlCommand().Parameters;

			oParameters.AddWithValue("@ID", CorrespondenceID);

			string query = "select * from auditheader h with (readpast)" +
				"left join auditdetail d on h.id = d.auditid where " +
				"h.correspondenceid = @ID order by id asc";

			return GetAuditList(query, oParameters, null, TDatabase.Client);
		}

		public static List<AuditLog> GetAuditListByAuditID(long AuditID) {
			return GetAuditListByAuditID(AuditID, TDatabase.Client);
		}


		public static List<AuditLog> GetAuditListByAuditID(long AuditID, TDatabase AuditDB) {
			SqlParameterCollection oParameters = new SqlCommand().Parameters;

			oParameters.AddWithValue("@ID", AuditID);

			string query = "select * from auditheader h with (readpast)" +
				"left join auditdetail d on h.id = d.auditid where " +
				"h.id = @ID order by id asc";

			return GetAuditList(query, oParameters, null, AuditDB);
		}


		public static List<AuditLog> GetAuditListByEvent(string type, long EventID) {
			SqlParameterCollection oParameters = new SqlCommand().Parameters;

			oParameters.AddWithValue("@ID", EventID);
			oParameters.AddWithValue("@Type", type);

			string query = "select * from auditheader h with (readpast)" +
				"left join auditdetail d on h.id = d.auditid where " +
				"h.DataType = @Type and h.EventID = @ID order by id asc";

			return GetAuditList(query, oParameters, null, TDatabase.Client);
		}

		#endregion

		#region private statics
		private static List<AuditLog> GetAuditList(string Q, SqlParameterCollection oParams, SqlUtil sql, TDatabase AuditDB) {
			if (sql == null)
				sql = SqlUtil.Get(AuditDB);

			List<AuditLog> headers = new List<AuditLog>();

			if ((long)oParams["@ID"].Value == 0)
				return headers;

			DataTable oTable = sql.ExecuteSingleResultSetSQLQuery(Q, oParams);
			if (oTable.Rows.Count == 0)
				return headers;

			//each unique header has a list of the details
			long CurrId = -1;
			long id;
			AuditFields fields = null;
			foreach (DataRow r in oTable.Rows) {
				id = DataUtils.LongZeroIfNull(r["ID"]);
				if (id != CurrId) {
					AuditLog a = new AuditLog(r, false);
					headers.Add(a);

					fields = a.mFieldChanges;
					CurrId = id;
				}

				fields.Add(r);
			}

			return headers;
		}
		#endregion

	}
}
