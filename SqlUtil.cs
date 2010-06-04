using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Diagnostics;

namespace SQLinqenlot {

	public class SqlUtilTransaction {
		internal SqlTransaction mTrans;
		internal SqlConnection mConnection;
		internal string mName;
		internal string mDatabase;
		internal string mDatabaseServer;

		public SqlUtilTransaction(SqlConnection c, string Server, string DBname, string name) {
			mTrans = c.BeginTransaction(IsolationLevel.ReadCommitted);
			mName = name;
			mConnection = c;
			mDatabase = DBname;
			mDatabaseServer = Server;
		}

		internal void SetWorkingDB(string db) {
			if (!mDatabase.Equals(db)) {
				mDatabase = db;
				mConnection.ChangeDatabase(db);
			}
		}

	}

	/// <summary>
	/// a utility class to talk to sql
	/// </summary>
	[Serializable()]
	public class SqlUtil : MarshalByRefObject {
		#region Private Constants
		/// <summary>
		/// The Number of times to retry opening a connection that has either failed to open, or has been broken
		/// </summary>
		private const int mnMAX_CONNECTION_RETRIES = 5;

		/// <summary>
		/// The Number of times to retry the execution of a command whose execution has failed.  
		/// </summary>
		private const int mnMAX_COMMAND_EXECUTION_RETRIES = 5;


		/// <summary>
		/// The Number of seconds to wait before retrying
		/// </summary>
		private const short SECONDS_TO_WAIT_BEFORE_A_COMMAND_RETRY = 5;

		#endregion

		#region Private Data Members
		/// <summary>
		/// The Database to which this service will bind.
		/// </summary>
		//this is never used by the class

		/// <summary>
		/// The internal SqlConnection object used to communicate with the target Database.
		/// </summary> 
		[NonSerialized()]
		private object mSynchLock = new object();
		private SqlConnection moDBConn;

		/// <summary>
		/// Holds the current & active connection string
		/// </summary>
		private string msActiveConnectionString = null;

		protected TDatabase mDatabase;
		protected string mDatabaseName;
		protected string msDatabaseServer;

		/// <summary>
		/// A Hashtable used to control transaction - this is thread safe
		/// </summary>
		[NonSerialized()]
		private static Dictionary<string, SqlUtilTransaction> moActiveTransactions = new Dictionary<string, SqlUtilTransaction>();
		[NonSerialized()]
		private static Dictionary<string, SqlUtil> mRegisteredConnection = new Dictionary<string, SqlUtil>();

		//by pass trans allows an application to write something to the DB and the rollback
		// wont affect it.  This is typicly uused for loginng errors
		private bool mBypassTransaction;
		public bool BypassTransaction {
			get { return mBypassTransaction; }
			set { mBypassTransaction = value; }
		}

		//for use in displaying the app name in master..sysprocesses table.
		//The class will prepend the hostname to this string for insert into that table
		//Defaults to processName
		private string ApplicationName;

		#endregion


		#region Constructors
		public string FullTablePath(string table) {
			if (!table.StartsWith("["))
				table = "[" + table + "]";

			return "[" + msDatabaseServer + "]." + DBName + "." + "dbo" + "." + table;
		}

		public static string BuildTablePath(TDatabase db, string table) {
			/*string svr = Syntac.RegistryUtility.GetRegistryValueAsString(Registry.LocalMachine, 
				@"Software\Syntac\Database\" + db.ToString(),"Location");

			return svr + "." + db.ToString() + ".dbo." + table;	
			*/
			return DBLocator.getTablePath(db, table);
		}

		//this is really a constructor for all those class that cant
		//change and add a new local variable
		//MULTI SAFE
		//TODO: In the future we may eliminate unspec as well
		public static SqlUtil Get(TDatabase Database) {
			return Get(Database, null);
		}

		public static SqlUtil Get(TDatabase Database, string appName) {
			bool NoCaching = true;
			if (NoCaching) {
				return new SqlUtil(Database, appName);
			}
			//save a copy of original for later use, in case first call to getDatabaseServer modifies it
			string db = Database.ToString();
			string svr = DBLocator.getDatabaseServer(ref db);
			string k = string.Format("{0}:{1}:{2}", Process.GetCurrentProcess().Id, svr, db);

			lock (xDBLock) {
				if (mRegisteredConnection[k] == null) {
					mRegisteredConnection[k] = new SqlUtil(Database, appName);
				}

				return (SqlUtil)mRegisteredConnection[k];
			}
		}

		public SqlUtil(string DBName) : this(DBName, null) { }

		public SqlUtil(TDatabase Database) : this(Database, null) { }

		public SqlUtil(string DBName, string appName) : this(TDatabase.Unknown, DBName, appName) { }

		public SqlUtil(TDatabase Database, string appName) : this(Database, Database.ToString(), appName) { }

		private SqlUtil(TDatabase Database, string DBName, string appName) {
			if (appName != null) {
				this.ApplicationName = appName;
			} else {
				this.ApplicationName = Process.GetCurrentProcess().ProcessName;
			}
			ConstructMe(Database, DBName, null);
		}

		public SqlUtil(string ServerName, string DBName, string appName) {
			if (appName != null) {
				this.ApplicationName = appName;
			} else {
				this.ApplicationName = Process.GetCurrentProcess().ProcessName;
			}

			ConstructMe(TDatabase.Unknown, DBName, ServerName);
		}

		// TODO: MULTI: decision on what db to use can be used here and only here.
		/// <summary>
		/// Initializes this object instance by creating an instance of the Internal SqlConnection object.  
		/// </summary>
		/// <param name="Database" >The Associated Database Name that this Data Service instance will be communicating with</param>
		/// <value>void</value>
		private void ConstructMe(TDatabase db, string DBName, string ServerName) {
			byte minConnect;
			byte maxConnect;

			//this call will do lots of cool things, but will give us back the name of the server that the caller needs.
			//it will also change the DBName if it is mapped to another actual name
			mDatabaseName = (db == TDatabase.Unknown ? DBName : db.ToString());
			mDatabase = db;
			if (ServerName != null)
				msDatabaseServer = ServerName;
			else
				msDatabaseServer = DBLocator.getDatabaseServer(ref mDatabaseName);

			DBLocator.getConnectionPoolMinMax(mDatabaseName, out minConnect, out maxConnect);

			try {
				msActiveConnectionString = "Application Name=" + this.ApplicationName + ";" +
					"server=" + msDatabaseServer + ";" +
					"trusted_Connection=yes;" +
					"Database=" + mDatabaseName + ";" +
					"connection reset=false;" +
					"enlist=true;" +
					"min pool size=" + minConnect + ";" +
					"max pool size=" + maxConnect;

				msActiveConnectionString = FixConnectionString(msActiveConnectionString);

				moDBConn = new SqlConnection(msActiveConnectionString);
			} catch (Exception ex) {
				//Lets Use The MS Application Block here w/a custom publisher that sends out an eMail & Publishes exception data to a structure in the Syntac Database.
				//If Syntac DB is not available then publish exception data to the event log.
				throw new Exception(ex.Message, ex);
			}
		}

		#endregion

		#region Public Transaction  Methods
		private int mTransactionCount = 0;
		private Stack<string> mTransactionStack = new Stack<string>();

		/// <summary>
		/// return true if we are in middle of a transaction
		/// </summary>
		/// <returns></returns>
		public bool InMiddleOfTransaction() {
			SqlUtilTransaction t = InTransaction();
			if (t == null)
				return false;

			return true;
		}

		/// <summary>
		/// Used to initiate a Transaction.  Will both create the transaction and add it to an internal  data
		/// structure that will track the Transaction's existence.
		/// </summary>
		/// <value>void</value>
		public void BeginTransaction() {
			string TransactionName = (++mTransactionCount).ToString();
			string k = BuildTransactionKey();

			lock (xDBLock) {
				SqlUtilTransaction t = (SqlUtilTransaction)moActiveTransactions[k];
				if (t == null) {
					OpenIfClosed();
					moActiveTransactions[k] = new SqlUtilTransaction(moDBConn, msDatabaseServer, mDatabaseName, TransactionName);
				}
			}
		}

		private int mTimeout;
		public void SetTimeoutForOneCall(int seconds) {
			//only sets this for next call - after that - back to default
			mTimeout = seconds;
		}

		public void CommitTransaction() {
			resolveTransaction(true);
		}

		public void RollBackTransaction() {
			resolveTransaction(false);
		}

		private string BuildTransactionKey() {
			return string.Format("{0}:{1}", Process.GetCurrentProcess().Id, msDatabaseServer.ToLower());
		}

		#region ExecuteNoResultSetStoredProcedure()

		/// <summary>
		/// Used to Execute a Stored Procedure that Does Not Return Any Tabular ResultSet(s)
		/// </summary>
		/// <param name="Parameters" >The Input/Output Parameters</param>
		/// <param name="StoredProcedureName" >The Name of the Stored Procedure to Call</param>
		/// <value>The Number of Affected Records as an Int</value>
		public int ExecuteNoResultSetStoredProcedure(string StoredProcedureName, SqlParameterCollection Parameters) {
			// THIS IS TEMP WORKAROUND FOR THE OPEN CONNECTION PROBLEM
			//SqlConnection c = OpenIfClosed();
			SqlCommand oCommand = new SqlCommand(StoredProcedureName);
			return (int)executeSP(oCommand, TResultSetTypes.None, Parameters);
		}

		/// <summary>
		/// Used to Execute a Stored Procedure that Does Not Return Any Tabular ResultSet(s)
		/// </summary>
		/// <param name="StoredProcedureName" >The Name of the Stored Procedure to Call</param>
		/// <value>The Number of Affected Records as an Int</value>
		public int ExecuteNoResultSetStoredProcedure(string StoredProcedureName) {
			// THIS IS TEMP WORKAROUND FOR THE OPEN CONNECTION PROBLEM
			//SqlConnection c = OpenIfClosed();
			SqlCommand oCommand = new SqlCommand(StoredProcedureName);
			return (int)executeSP(oCommand, TResultSetTypes.None, null);
		}

		#endregion

		#region ExecuteNoResultSetSQLQuery()
		public int ExecuteNoResultSetSQLQuery(string QueryText) {
			return ExecuteNoResultSetSQLQuery(QueryText, null);
		}
		public int ExecuteNoResultSetSQLQuery(string QueryText, SqlParameterCollection Parameters) {
			SqlCommand oCommand = new SqlCommand(QueryText);
			return (int)executeSQL(oCommand, TResultSetTypes.None, CommandType.Text, Parameters);
		}
		#endregion

		#region ExecuteSingleResultSetSQLQuery()
		public DataTable ExecuteSingleResultSetSQLQuery(string QueryText) {
			return ExecuteSingleResultSetSQLQuery(QueryText, null);
		}

		public DataTable ExecuteSingleResultSetSQLQuery(string QueryText, SqlParameterCollection Parameters) {
			SqlCommand oCommand;

			oCommand = new SqlCommand(QueryText);
			return (DataTable)executeSQL(oCommand, TResultSetTypes.Single, CommandType.Text, Parameters);
		}
		#endregion

		#region ExecuteSingleResultSetStoredProcedure()

		/// <summary>
		/// Used to Execute a Stored Procedure that Returns Just One Tabular ResultSet
		/// </summary>
		/// <param name="Parameters" >The Input/Output Parameters</param>
		/// <param name="StoredProcedureName" >The Name of the Stored Procedure to Call</param>
		/// <value>The Tabular ResultSet as A System.Data.DataTable Object</value>
		public DataTable ExecuteSingleResultSetStoredProcedure(string StoredProcedureName, SqlParameterCollection Parameters) {
			// THIS IS TEMP WORKAROUND FOR THE OPEN CONNECTION PROBLEM
			//SqlConnection c = OpenIfClosed();
			SqlCommand oCommand = new SqlCommand(StoredProcedureName);
			return (DataTable)executeSP(oCommand, TResultSetTypes.Single, Parameters);
		}

		/// <summary>
		/// Used to Execute a Stored Procedure that Returns Just One Tabular ResultSet
		/// </summary>
		/// <param name="StoredProcedureName" >The Name of the Stored Procedure to Call</param>
		/// <value>The Tabular ResultSet as A System.Data.DataTable Object</value>
		public DataTable ExecuteSingleResultSetStoredProcedure(string StoredProcedureName) {
			// THIS IS TEMP WORKAROUND FOR THE OPEN CONNECTION PROBLEM
			//SqlConnection c = OpenIfClosed();
			SqlCommand oCommand = new SqlCommand(StoredProcedureName);
			return (DataTable)executeSP(oCommand, TResultSetTypes.Single, null);
		}
		#endregion

		#region ExecuteMultipleResultSetSQLQuery()
		public DataSet ExecuteMultipleResultSetSQLQuery(string QueryText) {
			return ExecuteMultipleResultSetSQLQuery(QueryText, null);
		}

		public DataSet ExecuteMultipleResultSetSQLQuery(string QueryText, SqlParameterCollection Parameters) {
			SqlCommand oCommand;

			oCommand = new SqlCommand(QueryText);
			return (DataSet)executeSQL(oCommand, TResultSetTypes.Multiple, CommandType.Text, Parameters);
		}
		#endregion

		#region ExecuteMultipleResultSetStoredProcedure()

		/// <summary>
		/// Used to Execute a Stored Procedure that Returns Multiple Tabular ResultSets
		/// </summary>
		/// <param name="Parameters" >The Input/Output Parameters</param>
		/// <param name="StoredProcedureName" >The Name of the Stored Procedure to Call</param>
		/// <value>The Tabular ResultSets as A System.Data.DataSet Object</value>
		public DataSet ExecuteMultipleResultSetStoredProcedure(string StoredProcedureName, SqlParameterCollection Parameters) {
			// THIS IS TEMP WORKAROUND FOR THE OPEN CONNECTION PROBLEM
			//SqlConnection c = OpenIfClosed();
			SqlCommand oCommand = new SqlCommand(StoredProcedureName);
			return (DataSet)executeSP(oCommand, TResultSetTypes.Multiple, Parameters);
		}

		/// <summary>
		/// Used to Execute a Stored Procedure that Returns Multiple Tabular ResultSets
		/// </summary>
		/// <param name="StoredProcedureName" >The Name of the Stored Procedure to Call</param>
		/// <value>The Tabular ResultSets as A System.Data.DataSet Object</value>
		public DataSet ExecuteMultipleResultSetStoredProcedure(string StoredProcedureName) {
			// THIS IS TEMP WORKAROUND FOR THE OPEN CONNECTION PROBLEM
			//SqlConnection c = OpenIfClosed();
			SqlCommand oCommand = new SqlCommand(StoredProcedureName);
			return (DataSet)executeSP(oCommand, TResultSetTypes.Multiple, null);
		}
		#endregion

		#region ExecuteScalarResultSetStoredProcedure()

		/// <summary>
		/// Used to Execute a Stored Procedure that Returns A Scalar Value.  That is the first field value in the
		/// first row of the first resultset 
		/// </summary>
		/// <param name="Parameters" >The Input/Output Parameters</param>
		/// <param name="StoredProcedureName" >The Name of the Stored Procedure to Call</param>
		/// <value>The Scalar result as an object</value>
		public object ExecuteScalarResultSetStoredProcedure(string StoredProcedureName, SqlParameterCollection Parameters) {
			// THIS IS TEMP WORKAROUND FOR THE OPEN CONNECTION PROBLEM
			//SqlConnection c = OpenIfClosed();
			SqlCommand oCommand = new SqlCommand(StoredProcedureName);
			return executeSP(oCommand, TResultSetTypes.Scalar, Parameters);
		}

		/// <summary>
		/// Used to Execute a Stored Procedure that Returns A Scalar Value.  That is the first field value in the
		/// first row of the first resultset 
		/// </summary>
		/// <param name="StoredProcedureName" >The Name of the Stored Procedure to Call</param>
		/// <value>The Scalar result as an object</value>
		public object ExecuteScalarResultSetStoredProcedure(string StoredProcedureName) {
			// THIS IS TEMP WORKAROUND FOR THE OPEN CONNECTION PROBLEM
			//SqlConnection c = OpenIfClosed();
			SqlCommand oCommand = new SqlCommand(StoredProcedureName);
			return executeSP(oCommand, TResultSetTypes.Scalar, null);
		}
		#endregion

		#region ExecuteScalarResultSetSQLQuery
		public object ExecuteScalarResultSetSQLQuery(string QueryText) {
			return ExecuteScalarResultSetSQLQuery(QueryText, null);
		}

		public object ExecuteScalarResultSetSQLQuery(string QueryText, SqlParameterCollection Parameters) {
			SqlCommand oCommand = new SqlCommand(QueryText);
			return executeSQL(oCommand, TResultSetTypes.Scalar, CommandType.Text, Parameters);
		}
		#endregion

		#region TableExists
		public bool TableExists(string TblName) {
			SqlParameterCollection p = new SqlCommand().Parameters;
			p.AddWithValue("@ObjectName", TblName);
			p.AddWithValue("@ObjectType", "u");

			return ExecuteScalarResultSetSQLQuery("SELECT id from sysobjects WHERE name=@ObjectName and type=@ObjectType", p) != null;
		}
		#endregion

		#endregion

		#region Private Methods

		/// <summary>
		/// Resolves a Transaction, commiting if it is a success, and rolling back if its a failure
		/// </summary>
		/// <param name="IsSuccessFul" >A Boolean indicating wether or not a Transaction was Successful</param>
		/// <param name="TransactionName" >
		/// The Name of the related Transaction.  An Argument Exception will be thrown if a Transaction 
		/// with that name doesn't exist
		/// </param>
		/// <value>void</value>
		private void resolveTransaction(bool IsSuccessFul) {
			string k = BuildTransactionKey();

			SqlUtilTransaction t;
			lock (xDBLock) {
				t = (SqlUtilTransaction)moActiveTransactions[k];
			}

			if (t != null) {
				try {
					if (IsSuccessFul)
						t.mTrans.Commit();
					else
						t.mTrans.Rollback();

					t.mTrans.Dispose();

					lock (xDBLock) {
						moActiveTransactions.Remove(k);
					}

					//there are two possible connections here - one that is from this SQL instance
					// and one from the transaction
					CloseActiveConnection(ref t.mConnection);
					CloseActiveConnection(ref moDBConn);
				} catch (Exception x) {
					//must clear state of transaction even in error
					lock (xDBLock) {
						moActiveTransactions.Remove(k);
					}
					throw new Exception(x.Message, x);
				}
			}
		}



		#region This needs to go as it is a workaround until we can change the usage of data service objects to be shorter
		private void ConstructMeAgain() {
			moDBConn = new SqlConnection(msActiveConnectionString);
		}
		#endregion

		/// <summary>
		/// Used to tell if a SQLException occured as a result of a time-out or deadlock.
		/// </summary>
		/// <param name="ErrorText">The Text of the Related Error Message.  This text is inspected to see if the Error was Timeout or Deadlock related</param>
		/// <value>A Boolean indicating whether or not the Error was Time Related</value>
		public static bool isTimeRelatedError(string ErrorText) {
			return ErrorText.IndexOf("was deadlocked on lock") > -1 ||
				ErrorText.IndexOf("General network error") > -1 ||
				ErrorText.IndexOf("Timeout expired") > -1;
		}

		public static bool IsDBNull(object SourceObject) {
			return SourceObject != null && SourceObject.GetType() == typeof(System.DBNull);
		}

		public static Dictionary<string, object> xIndexExistsCache = new Dictionary<string, object>();
		/// <summary>
		/// check if a table has a specified index.
		/// </summary>
		/// <param name="TableName"></param>
		/// <param name="IndexName"></param>
		public bool DoesIndexExists(string TableName, string IndexName) {
			string k = this.DBServerName + ":" + this.DBName + ":" + TableName + ":" + IndexName;
			object o = xIndexExistsCache[k];
			if (o == null) {
				string q = String.Format("select idx.id from sysobjects objs " +
						"left join sysindexes idx on idx.ID = objs.ID " +
						"where objs.name = '{0}' and idx.name = '{1}'", TableName, IndexName);

				o = this.ExecuteScalarResultSetSQLQuery(q);
				if (o == null)
					o = false;
				else
					o = true;

				xIndexExistsCache[k] = o;
			}

			return (bool)o;
		}

		/// <summary>
		/// returns a comma sepraetd list including parens that is good for query in list, for example x in (a,c,v)
		/// will quote string data. If QuoetAllValues is on, all kinds of data in the list will be single quoted
		/// </summary>
		/// <param name="list"></param>
		/// <returns></returns>
		public static string CreateInList(List<object> list, bool QuoteAllValues) {
			if (list == null || list.Count == 0)
				throw new Exception("array list must have some elements");

			string l = "(";
			foreach (object o in list) {
				System.Type t = o.GetType();
				if (QuoteAllValues || t == typeof(string) || t == typeof(char)) {
					l += "'" + o.ToString() + "',";
				} else {
					l += o.ToString() + ",";
				}
			}

			//chop off the last comma and add close paren
			return l.Substring(0, l.Length - 1) + ")";
		}


		/// <summary>
		/// Used to Connect to the Database.  This method is fault tolerant to Time related delays 
		/// </summary>
		/// <value>void</value>
		public void Connect() {
			for (int tries = 1; moDBConn.State != ConnectionState.Open; tries++) {
				try {
					moDBConn.Open();
				} catch (SqlException sqlEx) {
					if (isTimeRelatedError(sqlEx.ToString()) && tries < mnMAX_CONNECTION_RETRIES)
						continue;
					else
						throw sqlEx;
				}
			}
		}

		/// <summary>
		/// Checks to see if the internal SqlConnection object is Opened.  If not, then it will try to open it.
		/// </summary>
		/// <value>void</value>
		private SqlConnection OpenIfClosed() {
			return OpenIfClosed(null);
		}

		private SqlConnection OpenIfClosed(SqlCommand cmd) {

			SqlUtilTransaction t = InTransaction();
			if (t != null) {
				// since one transaction could span multiple Databases (on one server), we have to make sure we
				// are using the proper database for this request
				t.SetWorkingDB(DBName);

				if (cmd != null) {
					cmd.Transaction = t.mTrans;
					cmd.Connection = t.mConnection;
				}

				if (t.mConnection.State == ConnectionState.Closed || t.mConnection.State == ConnectionState.Broken)
					throw (new Exception("connection closed in middle of transaction"));

				return t.mConnection;
			}

			// THIS IS TEMP WORKAROUND FOR THE OPEN CONNECTION PROBLEM
			if (moDBConn == null)
				ConstructMeAgain();
			if (moDBConn.State == ConnectionState.Closed || moDBConn.State == ConnectionState.Broken)
				this.Connect();

			if (cmd != null)
				cmd.Connection = moDBConn;

			//we think we have reconnected - but if connection pooling is on, we really just
			// get the last connection - which may be pointing to a diff database,
			// so we insure its set over here
			if (!moDBConn.Database.Equals(DBName))
				moDBConn.ChangeDatabase(DBName);

			return moDBConn;
		}



		/// <summary>
		/// Executes a stored Procedure on the Associated Database.  The Execution is Fault Tolerant to Time-Out or Deadlock related Errors.
		/// </summary>
		/// <param name="Parameters" >The Input/Output Parameters</param>
		/// <param name="ResultSetType" >The Desired Result Set Type</param>
		/// <param name="StoredProcedure" >A Command Object encapsulating the Stored Procedure Call</param>
		/// <value>An Object containing the Desired Result Set Type</value>
		private object executeSP(SqlCommand SqlCommand, TResultSetTypes ResultSetType, SqlParameterCollection Parameters) {
			return executeSQL(SqlCommand, ResultSetType, CommandType.StoredProcedure, Parameters);
		}

		private static object xDBLock = new object();
		/// <summary>
		/// Executes any SQL statement on the database.
		/// </summary>
		/// <param name="Parameters" >The Input/Output Parameters</param>
		/// <param name="ResultSetType" >The Desired Result Set Type</param>
		/// <param name="StoredProcedure" >A Command Object encapsulating the Stored Procedure Call</param>
		/// <param name="SQLCommandType">The SQL command type.</param>
		/// <value>An Object containing the Desired Result Set Type</value>
		private object executeSQL(SqlCommand SqlCommand, TResultSetTypes ResultSetType, CommandType SQLCommandType, SqlParameterCollection Parameters) {
			SqlDataAdapter da = null;
			DataTable returnDt = null;
			DataSet returnDs = null;
			Exception SaveException = null;

			if (mTimeout != 0) {
				SqlCommand.CommandTimeout = mTimeout;
				mTimeout = 0;
			}
			lock (mSynchLock) {
				try {
					OpenIfClosed(SqlCommand);

					SqlCommand.CommandType = SQLCommandType;
					if (Parameters != null) {
						List<SqlParameter> plist = new List<SqlParameter>();
						foreach (SqlParameter p in Parameters) {
							if (!this.NVarcharAllowed && p.SqlDbType == SqlDbType.NVarChar)
								p.SqlDbType = SqlDbType.VarChar;

							plist.Add(p);
						}
						Parameters.Clear();
						SqlCommand.Parameters.AddRange(plist.ToArray());
					}
					if (ResultSetType != TResultSetTypes.None && ResultSetType != TResultSetTypes.Scalar) {
						da = new SqlDataAdapter(SqlCommand);
					}
					for (int i = 1; i <= mnMAX_COMMAND_EXECUTION_RETRIES; i++) {
						OpenIfClosed(SqlCommand);
						try {
							switch (ResultSetType) {
								case TResultSetTypes.Scalar:
									return SqlCommand.ExecuteScalar();
								case TResultSetTypes.None:
									return SqlCommand.ExecuteNonQuery();
								case TResultSetTypes.Single:
									returnDt = new DataTable();
									fillFromAdapter(da, returnDt);
									return returnDt;
								case TResultSetTypes.Multiple:
									returnDs = new DataSet();
									fillFromAdapter(da, returnDs);
									return returnDs;
								default:
									throw new EnumerationValueException(ResultSetType);
							}
						} catch (SqlException sqlEx) {
							if (InTransaction() != null)
								throw sqlEx;

							if ((isTimeRelatedError(sqlEx.ToString()) ||
								sqlEx.Message == "Unknown error." ||
								sqlEx.Message == "A severe error occurred on the current command.  The results, if any, should be discarded.") && i < mnMAX_COMMAND_EXECUTION_RETRIES) {
								AuditLog.AuditException((Exception)sqlEx, "sql will retry", null);
								Thread.Sleep(TimeSpan.FromSeconds(SECONDS_TO_WAIT_BEFORE_A_COMMAND_RETRY));
								continue;
							} else
								throw sqlEx;
						} catch (InvalidOperationException iEx) {
							if (InTransaction() != null)
								throw iEx;

							if ((isTimeRelatedError(iEx.ToString()) ||
								iEx.Message == "Unknown error." ||
								iEx.Message == "A severe error occurred on the current command.  The results, if any, should be discarded.") && i < mnMAX_COMMAND_EXECUTION_RETRIES) {
								AuditLog.AuditException((Exception)iEx, "sql will retry", null);
								Thread.Sleep(TimeSpan.FromSeconds(SECONDS_TO_WAIT_BEFORE_A_COMMAND_RETRY));
								continue;
							} else
								throw iEx;
						}
					}
				} catch (SqlException sqlEx) {
					SaveException = (Exception)sqlEx;
					//Lets Use The MS Application Block here w/a custom publisher that sends out an eMail & Publishes exception data to a structure in the Syntac Database.
					//If Syntac DB is not available then publish exception data to the event log.
					throw sqlEx;
				} catch (Exception ex) {
					SaveException = ex;
					//Lets Use The MS Application Block here w/a custom publisher that sends out an eMail & Publishes exception data to a structure in the Syntac Database.
					//If Syntac DB is not available then publish exception data to the event log.
					throw new Exception(ex.Message, ex);
				} finally {
					if (SaveException != null)
						AuditLog.AuditException(SaveException, "sqlutil failure", null);

					if (da != null)
						da.Dispose();
					if (returnDt != null)
						returnDt.Dispose();
					if (returnDs != null)
						returnDs.Dispose();
					if (SqlCommand != null)
						SqlCommand.Dispose();
					// Close Active Connection to Database
					CloseActiveConnection();
				}
				return null;
			}
		}

		private void fillFromAdapter(SqlDataAdapter SourceAdapter, object Target) {
			if (Target.GetType() != typeof(System.Data.DataTable) && Target.GetType() != typeof(System.Data.DataSet))
				throw new ArgumentException("target must be of type System.Data.DataTable or System.Data.DataSet", "target");
			//Added for Increased Error Handling & Thread Safety
			bool bSuccess = false;
			for (int retries = 0; !bSuccess && retries <= mnMAX_COMMAND_EXECUTION_RETRIES; retries++) {
				try {
					if (Target.GetType() == typeof(System.Data.DataTable)) {
						SourceAdapter.Fill((System.Data.DataTable)Target);
						bSuccess = true;
					} else {
						SourceAdapter.Fill((System.Data.DataSet)Target);
						bSuccess = true;
					}
				} catch (System.InvalidOperationException iEx) {
					if ((iEx.Message == "There is already an open DataReader associated with this Connection which must be closed first." ||
						iEx.Message == "Internal connection fatal error.") && retries < mnMAX_COMMAND_EXECUTION_RETRIES) {
						Thread.Sleep(TimeSpan.FromSeconds(SECONDS_TO_WAIT_BEFORE_A_COMMAND_RETRY));
						continue;
					} else
						throw iEx;
				} catch (SqlException sqlEx) {
					if (sqlEx.Message.Equals("A severe error occurred on the current command.  The results, if any, should be discarded.")
						&& retries < mnMAX_COMMAND_EXECUTION_RETRIES) {
						Thread.Sleep(TimeSpan.FromSeconds(SECONDS_TO_WAIT_BEFORE_A_COMMAND_RETRY));
						continue;
					} else
						throw sqlEx;
				} catch (Exception eX) {
					throw new Exception(eX.Message, eX);
				}
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// 
		protected void CloseActiveConnection() {
			CloseActiveConnection(ref moDBConn);
		}

		protected void CloseActiveConnection(ref SqlConnection c) {
			try {
				if (c != null && InTransaction() == null) {
					c.Close();
					c.Dispose();
					c = null;
				}
			} catch { }
		}

		private SqlUtilTransaction InTransaction() {
			if (mBypassTransaction)
				return null;

			lock (xDBLock) {
				string Key = BuildTransactionKey();
				if (moActiveTransactions.ContainsKey(Key))
					return moActiveTransactions[BuildTransactionKey()];
				return null;
			}
		}

		#endregion

		#region protected region
		static private bool mbDevelChecked = false;
		static private bool mbIsDevel = false;
		static public string FixConnectionString(string ConnStr) {
			if (!mbDevelChecked) {
				lock (xDBLock) {
					mbDevelChecked = true;
					mbIsDevel = false;
					try {
						mbIsDevel = DataUtils.ToBool(RegistryUtility.GetRegistryValueAsString(Registry.LocalMachine, @"Software\Syntac", "Devel"));
						//mbIsDevel = Convert.ToBoolean(Registry.LocalMachine.OpenSubKey(@"Software\Datanet", false).GetValue("Devel"));
					} catch { }
				}
			}

			if (mbIsDevel)
				ConnStr = ConnStr.Replace("trusted_Connection=yes;", "UID=Syntac-ro;PWD=readonly;");
			return ConnStr;
		}

		#endregion

		/// <summary>
		/// The database server on which the database is hosted.
		/// </summary>
		// never used should be removed
		/*		public DatabaseServer DatabaseServer {
			get {
				return meDatabaseServer;
			}
			set {
				meDatabaseServer = value;
			}
		}
		*/
		public string DBServerName {
			get {
				return msDatabaseServer;
			}
		}

		public string DBName {
			get {
				return mDatabaseName;
			}
		}

		public TDatabase Database {
			get { return mDatabase; }
		}

		//TODO: can this be used as an way for a user to change db's? Probably.
		public SqlConnection DBConnection {
			get { return moDBConn; }
		}

		//can only be used by DataService to get Conenctions tring.  We add in stuf to
		// the string, so the pooled conenction in Sqlutil *wont* get used by data services
		// the reason is dataserive at this point dont ensure they are on the right
		//database by doing a use.
		public string DBConnectionStr {
			get {
				return msActiveConnectionString.Replace("Application Name=", "Application Name=[DataService]");
			}
		}

		/// <summary>
		/// sqlutil will automaticly convert any sql paramater that has a string object to a varchar (instead of the default nvarchar).
		/// If this flag is on, sqlutil will not convert it
		/// </summary>
		public bool NVarcharAllowed = false;
	}
}
