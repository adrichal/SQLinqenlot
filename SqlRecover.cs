using System;
using System.IO;
using System.Data;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Reflection;
using Microsoft.Win32;
using System.Configuration;

namespace SQLinqenlot {
	/// <summary>
	/// Summary description for SqlRecover.
	/// </summary>
	public abstract class SqlRecover {

		private static string mDir = null;
		public static string GetRecoverDir() {
			if (mDir == null) {
				string d = ConfigurationManager.AppSettings.Get("RecoveryDir");
				if (string.IsNullOrEmpty(d))
					d = @"c:\temp\recovery\";

				DirectoryInfo di = new DirectoryInfo(d);
				if (!di.Exists)
					di.Create();

				if (!d.EndsWith(@"\"))
					d += @"\";

				mDir = d;
			}

			return mDir;
		}

		public static string BuildRecoverPath(string BaseFileName) {
			if (BaseFileName.EndsWith(".recover"))
				return GetRecoverDir() + BaseFileName;

			return GetRecoverDir() + BaseFileName + ".recover";
		}

		/// <summary>
		/// Check if x is 'recoverable' and if so save it
		/// </summary>
		/// <param name="x"></param>
		/// <param name="rethrow"></param>
		/// <param name="o"></param>
		/// <param name="BaseFileName"></param>
		/// <returns></returns>
		public static bool SaveForRecovery(Exception x, bool rethrow, object o, string BaseFileName) {
			if (SqlUtil.isTimeRelatedError(x.Message))
				return SaveForRecovery(o, BaseFileName);

			if (rethrow)
				throw new Exception("cant recover", x);

			return false;
		}

		public static bool SaveForRecovery(object o, string BaseFileName) {
			if (GetRecoveryCB(o) == null && BaseFileName != GenericTable.GT_AUTORECOVER)
				throw new Exception("no recovery CB defined - cant save!");

			return FileOfObjects.Write(o, BuildRecoverPath(BaseFileName));
		}

		public static MethodInfo GetRecoveryCB(object o) {
			Type myType = o.GetType();
			return myType.GetMethod("SqlRecoverCB");
		}

	}
}
