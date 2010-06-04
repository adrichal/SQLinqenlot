using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;

namespace SQLinqenlot {
	public static class ApplicationParameters {
		/*
		 The main method is GetParm(string appname, string ParmName) looks for the most specific match for a paramater. 
		 If none is found a null is returned.

		Clients DB
		App		parm	value
				p1		p1-client
		App1 	p1		app1's p1


		RMSC DB
		App		parm	value
		Global	p2		p2 is global parm

		Any app trying to get the value for p2 will get the system global value.
		All apps except App1 will get the value 'p1-client' when loading param p1.  App1 will get the value 'app1's p1'
		*/

		//need to store app parms keyed by client name so multi client apps can use this class
		private static Dictionary<string, Dictionary<string, Dictionary<string, string>>> mAllClientParms = new Dictionary<string, Dictionary<string, Dictionary<string, string>>>();

		private static Dictionary<string, Dictionary<string, string>> mParms;
		/// <summary>
		/// Clears the cache of application parameters, which will force a refresh from the DB next time a parameter is queried.
		/// </summary>
		public static void ClearCache() {
			mAllClientParms.Clear();
		}
		/// <summary>
		/// paramater are gotten from the ApplicationParamater table in the client and RMSC DBs.  the client DB can have values that are global to that client or
		/// parms for spcific application.  
		/// </summary>
		private static void LoadParms() {
			mParms = mAllClientParms[DBLocator.ActiveClientName];
			if (mParms != null)
				return;

			mParms = new Dictionary<string, Dictionary<string, string>>();
			mAllClientParms[DBLocator.ActiveClientName] = mParms;

			Dictionary<string, string> SharedClientParms = new Dictionary<string, string>();
			Dictionary<string, string> GlobalParms = new Dictionary<string, string>();

			mParms[".client"] = SharedClientParms;
			mParms[".global"] = GlobalParms;

			SqlUtil sql = new SqlUtil(TDatabase.Client);
			string TableName = "ApplicationParameter";
			//need to specila case RMSC since the applicationparater table in RMSC is used for default values for all client for all apps
			//the problem is RMSC has aps of its own that its needs to configure.
			if (DBLocator.ActiveClientName == "RMSC")
				TableName = "RMSC_" + TableName;

			DataTable dt = sql.ExecuteSingleResultSetSQLQuery("select * from " + TableName + " order by App");

			string AppName;
			string parm;
			string val;
			Dictionary<string, string> dict;
			foreach (DataRow r in dt.Rows) {
				AppName = (string)DataUtils.BlankIfNull(r["App"]);
				parm = (string)r["Parm"];
				val = (string)r["Value"];

				if (AppName == "")
					dict = SharedClientParms;
				else {
					dict = mParms[AppName];
					if (dict == null) {
						dict = new Dictionary<string, string>();
						mParms[AppName] = dict;
					}
				}

				dict[parm.ToLower()] = val;
			}

			//go to EISS and only get the GLOBAL
			sql = new SqlUtil(TDatabase.Shared);
			dt = sql.ExecuteSingleResultSetSQLQuery("select * from ApplicationParameter");
			foreach (DataRow r in dt.Rows) {
				AppName = (string)DataUtils.BlankIfNull(r["App"]);
				parm = ((string)r["Parm"]).ToLower();
				val = (string)r["Value"];

				if (AppName == "global")
					dict = GlobalParms;
				else {
					dict = mParms[AppName];
					if (dict == null) {
						dict = new Dictionary<string, string>();
						mParms[AppName] = dict;
					}

					//dont allow parms found in the RMSC app parms table to override client settings
					if (dict.ContainsKey(parm))
						continue;
				}

				dict[parm] = val;
			}
		}
		/// <summary>
		/// gets parm and uses client and global for defaults
		/// </summary>
		/// <param name="AppName"></param>
		/// <param name="ParmName"></param>
		/// <returns></returns>
		public static string GetParm(string AppName, string ParmName) {
			return GetParm(AppName, ParmName, false);
		}

		/// <summary>
		/// Gets a parmater based on the application name and paramaters name
		/// First it checks application, then client shared, and then RMSC global
		/// </summary>
		/// <param name="AppName"></param>
		/// <param name="ParmName"></param>
		/// <param name="NoDefaults">If a value is not found for this app or this client, we do not look at the client or global cfg</param>
		/// <returns>Be warned a null can be returned!</returns>
		public static string GetParm(string AppName, string ParmName, bool NoDefaults) {
			LoadParms();

			ParmName = ParmName.ToLower();

			string val = null;
			if (AppName == null)
				AppName = "__we _should _ not find this key";

			string WhichHash = AppName;
			Dictionary<string, string> AppHash;
			for (int j = 0; j < 2; j++) {
				AppHash = mParms[WhichHash];
				if (AppHash != null)
					val = (string)AppHash[ParmName];

				if (val != null)
					return val;

				if (NoDefaults)
					return null;

				//next time thru the loop check client level  parms
				WhichHash = ".client";
			}

			//check global hash
			val = mParms[".global"][ParmName];
			return val;

		}

		/// <summary>
		/// adds a parmater to client's application paramater table.  Mostly used by SetupClient
		/// </summary>
		/// <param name="app"></param>
		/// <param name="parm"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public static void Add(string app, string parm, string value) {
			string p = ApplicationParameters.GetParm(app, parm);
			if (p != null) {
				if (p == value)
					return;

				throw new Exception("parm already exists - cant change value");
			}

			string i = String.Format("insert into ApplicationParameter (app, parm, [value]) values ('{0}', '{1}', '{2}')",
				app, parm, value);

			SqlUtil sql = new SqlUtil(TDatabase.Client);
			sql.ExecuteNoResultSetSQLQuery(i);
		}


	}


}
