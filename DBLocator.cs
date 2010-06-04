using System;
using Microsoft.Win32;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;

namespace SQLinqenlot {
	/// <summary>
	/// DBLocator contains static methods for locating a database server and mapping a logical DB name to an actual db name.
	/// </summary>
	public static class DBLocator {
		const byte MIN_POOL_SIZE = 5;
		const byte MAX_POOL_SIZE = 10;

		static DBLocator() {
			__ServerMap = new UniStatic<DataTable>();
			__SyncLock = new UniStatic<object>();
			__ActiveClient = new UniStatic<SyntacClient>();
		}

		private static UniStatic<DataTable> __ServerMap;
		private static DataTable ServerMap {
			get { 
				return __ServerMap.Value; 
			}
			set { __ServerMap.Value = value; }
		}

		private static UniStatic<object> __SyncLock;
		private static object SyncLock {
			get {
				if (__SyncLock.Value == null)
					__SyncLock.Value = new object();
				return __SyncLock.Value; 
			}
		}

		private enum PoolSize {
			Min,
			Max
		}


		/// <summary>
		/// only needed when a new client is create for now.
		/// </summary>
		public static void ClearServerCache() {
			ServerMap = null;
		}

		public static long ActiveClientID {
			get {
				if (ActiveClient == null)
					return 0;
				return ActiveClient.ID;
			}
			set {
				lock (SyncLock) {
					if (ActiveClientID != value) {
						var newClient = SyntacClient.GetByID(value);
						if (newClient == null)
							throw new InvalidOperationException("Invalid Client ID: " + value.ToString());
						ActiveClient = newClient;
					}
				}
			}
		}

		private static UniStatic<SyntacClient> __ActiveClient;
		public static SyntacClient ActiveClient {
			get {
				return __ActiveClient.Value;
			}
			private set {
				__ActiveClient.Value = value;
			}
		}

		public static string ActiveClientName {
			get {
				return ActiveClient.Name;
			}
			set {
				lock (SyncLock) {
					if (ActiveClient == null || ActiveClient.Name != value) {
						SyntacClient c = SyntacClient.GetByName(value);
						if (c == null) {
							throw new ApplicationException("Invalid Client Name: " + value);
						}
						ActiveClient = c;
					}
				}
			}
		}

		/// <summary>
		/// Returns a fully qualified path to the specified table.
		/// </summary>
		/// <param name="LogicalDBName"></param>
		/// <param name="tableName"></param>
		/// <returns></returns>
		public static string getTablePath(TDatabase db, string tableName) {
			string ActualDBName = db.ToString();
			string dbServer = getDatabaseServer(ref ActualDBName);

			if (tableName.Substring(0, 1) != "[")
				tableName = "[" + tableName + "]";
			if (dbServer.Substring(0, 1) != "[")
				dbServer = "[" + dbServer + "]";

			return string.Format("{0}.{1}.dbo.{2}", dbServer, ActualDBName, tableName);

		}
		/// <summary>
		/// Given a database name, determine which server it sits on. Aside from returning the db server
		/// it also will set logical dataBaseName to actual for client specific db's. 
		/// (Client specific db's like poc or GECS may have client specific names lile CITI_GECS.
		/// 
		/// First check for presence of a reg value for this specific DB. If not found...
		/// Second, check  DatabaseMap table in Syntac db for a row that matches our Client and the db we want. If not found
		/// Third, check for default (non-client specific, id=0) mapping for a db (this is what will happen for all GLOBAL db's). 
		/// </summary>
		/// <param name="databaseName">Logical name of database. For client db's will change it to actual name</param>
		/// <returns>database server name</returns>
		public static string getDatabaseServer(ref string databaseName) {

			if (databaseName == null)
				throw new NoNullAllowedException("Database name cannot be null.");

			string dbServer = null;

			try {
				//Check registry for local overides
				dbServer = getServerNameFromRegistry(ref databaseName, true);
			} catch (System.SystemException SE) {
				//Attempt to catch all the ex's that can happen when opening a reg value.
				throw new System.SystemException("Unable to read DB registry value. " + SE.Message, SE);
			} catch {
				/*	A general exception is thrown by the Reg Util if the value specified does not exist.
					Assuming this is not an error; there just is no reg entry for this dv.				
					Now then we must look for an entry for the main Syntac database, the source of all db information in the world.
					From it we will populate a local cache of the table. 
				*/
				lock (SyncLock) {
					if (ServerMap == null) {
						populateServerTable();
					}
				}

				//lookup will also translate logicalName to actual name
				dbServer = serverLookup(ref databaseName, ActiveClientName);

				if (dbServer == null) {
					throw new ApplicationException("Database not found Exception.");
				}
			}

			return dbServer;
		}

		private static string getServerNameFromRegistry(ref string dbName, bool causeException) {
			string KeyPath = @"Software\Syntac\Database\" + dbName;
			string ActualDBName = RegistryUtility.GetRegistryValueAsString(Registry.LocalMachine, KeyPath, "ActualDBName", false);
			if (!string.IsNullOrEmpty(ActualDBName))
				dbName = ActualDBName;
			return RegistryUtility.GetRegistryValueAsString(Registry.LocalMachine, KeyPath, "Location", causeException);
		}

		private const string CLIENT_NAME_REG_KEY = @"Software\Syntac\";
		private const string CLIENT_NAME_REG_VALUE = "ActiveClient";

		public static string getClientNameFromRegistry() {
			return RegistryUtility.GetRegistryValueAsString(Registry.LocalMachine, CLIENT_NAME_REG_KEY, CLIENT_NAME_REG_VALUE, false);
		}

		public static void setClientNameInRegistry(string ClientName) {
			RegistryUtility.SetRegistryValue(Registry.LocalMachine, CLIENT_NAME_REG_KEY, CLIENT_NAME_REG_VALUE, ClientName);
		}

		/// <summary>
		/// Populates a local cache of the server table from the DB. Calling this will always force a new db lookup
		/// </summary>
		/// <param name="dbServer"></param>
		private static void populateServerTable() {
			SqlDataAdapter da;
			string SyntacDBServer = null;
			string connectionString = null;

			string sqlString = "SELECT dbsm.*, c.[Name] AS clientName"
				+ " FROM DatabaseServerMap dbsm"
				+ " JOIN Client c ON dbsm.ClientID = c.ID";

			//need Syntac global db server name 
			string SyntacDbName = "Syntac";
			try {
				if ((SyntacDBServer = getServerNameFromRegistry(ref SyntacDbName, false)) == null) {
					throw new Exception("Could not determine Syntac DB Server master configuration.");
				}
			} catch (System.SystemException SE) {
				throw new System.SystemException("Unable to read registry value for Syntac db. " + SE.Message, SE);
			}

			//			connectionString = "Application Name= DB Locator service; server=" + SyntacDBServer + ";" + "UID=sa;PWD=r3c0^3r7;" + "Database=Syntac";
			string DBCredentials = "trusted_Connection=yes;";
			connectionString = string.Format("Application Name= DB Locator service; server={0};{1}Database={2}", SyntacDBServer, DBCredentials, SyntacDbName);
			connectionString = SqlUtil.FixConnectionString(connectionString);
			ServerMap = new System.Data.DataTable("ServerMap");
			da = new SqlDataAdapter(sqlString, connectionString);
			da.Fill(ServerMap);
			da.Dispose();

		}

		/// <summary>
		/// Perform lookup in the cached DB map table. Change logicalName of database to actual name
		/// </summary>
		/// <param name="dbName">Logical name of the Database to find server for</param>
		/// <param name="clientName">Name of the client that needs server</param>
		/// <returns></returns>
		private static string serverLookup(ref string dbName, string clientName) {
			DataRow[] foundRows;

			//first check for a row for this client
			string filter = "clientName = '" + clientName + "' AND LogicalDBName = '" + dbName + "'";

			//this should only return one row
			//TODO: check for only one row, exception out if more than 1
			lock (SyncLock) {
				foundRows = ServerMap.Select(filter);
			}

			if (foundRows.Length > 1)
				throw new Exception("More than one match found in db lookup. Db mapping table may be corrupt!");

			//if nothing found check for default client
			if (foundRows.Length == 0) {
				filter = "clientName = '__DEFAULT' AND LogicalDBName = '" + dbName + "'";
				lock (SyncLock) {
					foundRows = ServerMap.Select(filter);
				}

				if (foundRows.Length == 0)
					return null;
			}

			dbName = (string)foundRows[0]["DBName"];
			return (string)foundRows[0]["ServerName"];
		}

		/// <summary>
		/// Determines the minimum and maximum connections in a pool.
		/// The method first checks the registry for database specific values (database override).
		/// If both or one of min/max is not specified in a Database override, the top level Database key
		/// is searched for a default value.
		/// If any of the values are still not known, a default value is used. Defined in MAX_POOL_SIZE/MIN_POOL_SIZE.
		///
		/// </summary>
		/// <param name="actualDBName"> The name of the database used should be that of the actual database 
		/// and not a logical one (eg. It should be the name set by getDatabaseServer via the databaseName 
		/// reference passed into the method).</param>
		/// <param name="min"> Byte into which the minimum size of the connection pool is stored.</param>
		/// <param name="max"> Byte into which the minimum size of the connection pool is stored.</param>
		/// <returns></returns>
		/// 

		public static void getConnectionPoolMinMax(string actualDBName, out byte min, out byte max) {
			min = getXPoolSize(actualDBName, PoolSize.Min);
			max = getXPoolSize(actualDBName, PoolSize.Max);
		}

		private static byte getXPoolSize(string dbName, PoolSize x) {
			string pSz;
			byte xPSz;

			if ((pSz = RegistryUtility.GetRegistryValueAsString(Registry.LocalMachine, "Software\\Syntac\\Database\\" + dbName, x.ToString() + " Connections", false)) == null) {
				if ((pSz = RegistryUtility.GetRegistryValueAsString(Registry.LocalMachine, "Software\\Syntac\\Database", x.ToString() + " Connections", false)) == null) {
					//for better program flow just convert this to string for now
					pSz = x == PoolSize.Max ? System.Convert.ToString(MAX_POOL_SIZE) : System.Convert.ToString(MIN_POOL_SIZE);
				}
			}

			try {
				xPSz = System.Convert.ToByte(pSz);
			} catch (System.OverflowException) {
				throw new Exception("Database " + dbName + " " + x.ToString() + " Pool size is of \"" + pSz + "\" is out of range.");
			} catch (System.FormatException) {
				throw new Exception("Database " + dbName + " " + x.ToString() + " Pool size is of \"" + pSz + "\" is not valid.");
			}

			return xPSz;
		}

	}
}
