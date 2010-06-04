using System;

namespace SQLinqenlot {
	/// <summary>
	/// Summary description for DBMetaData.
	/// </summary>
	public static class DBMetaData {
		public static bool isColumnEncrypted(string FullTablePath, string column) {
			if (column.ToLower() == "encryptedpassword" && (FullTablePath.ToLower().EndsWith(".dbo.[user]"))) {
				return true;
			} else {
				return false;
			}
		}
	}
}
