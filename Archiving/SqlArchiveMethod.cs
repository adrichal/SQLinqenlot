using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SQLinqenlot.Archiving {
	/// <summary>
	/// Summary description for SqlArchiveMethod.
	/// </summary>
	public class SqlArchiveMethod : ArchiveMethod {
		private string dbServer;
		private string dbTable;
		private string dbDatabase;
		private readonly string insertSql;
		private readonly string deleteSql;
		private readonly string selectSql;

		SqlUtil squtil;

		public SqlArchiveMethod(ArchiveControl ctl)
			: base(ctl) {
			string[] split = ctl.ArchiveInfo.Split(new Char[] { ';' });
			dbServer = split[0];
			dbDatabase = split[1];
			dbTable = split[2];

			mArchiveControl = ctl;

			insertSql = "Insert into " + dbTable + " (RecID,ControlID,data) Values(@ID,@controlID,@data)";
			deleteSql = "Delete " + dbTable + " where RecID = @ID AND controlID = @controlID";
			selectSql = "Select data from " + dbTable + " where RecID = @ID AND controlID = @controlID";
			squtil = new SqlUtil(dbServer, dbDatabase, "sql database archiver");
		}

		public override void WriteHash(long ID, Dictionary<string, object> data) {

			this.DeleteHash(ID);

			GenericTable gt = new GenericTable(squtil, dbTable, mArchiveControl.cfg.IDColumn);
			foreach (KeyValuePair<string,object> kvp in data) {
				if (kvp.Key == mArchiveControl.cfg.IDColumn)
					continue;

				if (kvp.Value != DBNull.Value)
					gt[kvp.Key] = kvp.Value;
			}

			gt.ID = ID;
			gt.CreateRecord();
		}


		public override void Write(long id, byte[] data) {
			SqlParameterCollection prms = new SqlCommand().Parameters;

			this.Delete(id);
			prms.AddWithValue("@ID", id);
			prms.AddWithValue("@controlID", mArchiveControl.ID);
			prms.AddWithValue("@data", data);
			squtil.ExecuteNoResultSetSQLQuery(insertSql, prms);
		}

		public override Dictionary<string, object> ReadHash(long ID) {
			//cant use GT cause GT calls archive to read data and that would create a recursive loop
			SqlParameterCollection prms = new SqlCommand().Parameters;
			prms.AddWithValue("@id", ID);
			DataTable dt = squtil.ExecuteSingleResultSetSQLQuery(String.Format("select * from {0} where {1} = @id", dbTable, mArchiveControl.cfg.IDColumn), prms);
			if (dt.Rows.Count == 0)
				return null;

			GenericTable gt = new GenericTable(squtil, dbTable, mArchiveControl.cfg.IDColumn);
			gt.Load(dt.Rows[0]);
			//this was not archived - it was just put there so we have the ability to archive that data
			if (this.mArchiveControl.ArchiveType == ArchiveControl.ArchiveTypeEnum.Column)
				gt.Data.Remove(mArchiveControl.cfg.AgeDeterminingColumn);
			return gt.Data;
		}

		public override byte[] Read(long ID) {
			SqlParameterCollection prms = new SqlCommand().Parameters;

			prms.AddWithValue("@ID", ID);
			prms.AddWithValue("@controlID", mArchiveControl.ID);
			return (byte[])squtil.ExecuteScalarResultSetSQLQuery(selectSql, prms);
		}

		public override int DeleteHash(long ID) {
			SqlParameterCollection prms = new SqlCommand().Parameters;
			prms.AddWithValue("@ID", ID);
			return squtil.ExecuteNoResultSetSQLQuery(String.Format("delete from {0} where {1} = @id", dbTable, mArchiveControl.cfg.IDColumn), prms);
		}

		public override int Delete(long ID) {
			SqlParameterCollection prms = new SqlCommand().Parameters;
			prms.AddWithValue("@ID", ID);
			prms.AddWithValue("@controlID", mArchiveControl.ID);
			return squtil.ExecuteNoResultSetSQLQuery(deleteSql, prms);
		}
	}
}
