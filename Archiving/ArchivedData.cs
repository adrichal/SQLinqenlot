using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using ICSharpCode.SharpZipLib;
using ICSharpCode.SharpZipLib.Zip.Compression;
using ICSharpCode.SharpZipLib.Zip.Compression.Streams;

namespace SQLinqenlot.Archiving {
	/// <summary>
	/// class used by application to get at archived data - mostly GT will use this.
	/// </summary>
	[Serializable()]
	public class ArchivedData {
		public ArchiveControl mArchiveControl;
		public long mID;

		private Dictionary<string, object> mData;
		public Dictionary<string, object> Data {
			get { return mData; }
		}

		private ArchivedData(ArchiveControl archive, long id) {
			mArchiveControl = archive;
			mID = id;

			this.Read();
		}

		public static ArchivedData ReadRecord(string TablePath, long ID) {
			//to avoid infinite loop - we alway return null for request for the archiove tables themselevs
			TablePath = TablePath.ToLower();
			if (TablePath.IndexOf("archivecontrol") > 0)
				return null;

			ArchiveControl a = ArchiveControl.Get(TablePath, ID);
			if (a == null)
				return null;

			ArchivedData ad = new ArchivedData(a, ID);
			if (ad.mData == null)
				return null;

			return ad;
		}

		public static ArchivedData ReadColumns(string TablePath, GenericTable rec) {
			//to avoid infinite loop - we alway return null for request for the archiove tables themselevs
			TablePath = TablePath.ToLower();
			if (TablePath.IndexOf("archivecon") > 0)
				return null;

			ArchiveConfig cfg = ArchiveConfig.Get(TablePath);
			if (cfg == null)
				return null;

			string Column = cfg.AgeDeterminingColumn;
			if (Column == null)
				return null;

			ArchiveControl a = ArchiveControl.Get(TablePath, (DateTime)rec.DataRow[Column]);
			if (a == null)
				return null;

			ArchivedData ad = new ArchivedData(a, rec.ID);
			if (ad.mData == null)
				return null;

			return ad;
		}

		/// <summary>
		/// a methid that lets you get at archive comlumn info without a date.  It just checks 
		/// all column archives for this table path
		/// </summary>
		/// <param name="TablePath"></param>
		/// <param name="id"></param>
		/// <returns></returns>
		public static ArchivedData ScanAllColumnArchives(string TablePath, long id) {
			//to avoid infinite loop - we alway return null for request for the archiove tables themselevs
			TablePath = TablePath.ToLower();
			if (TablePath.IndexOf("archivecon") > 0)
				return null;

			ArchiveConfig cfg = ArchiveConfig.Get(TablePath);
			if (cfg == null)
				return null;

			ArchiveControlList l = ArchiveControl.GetControlList(TablePath);
			foreach (ArchiveControl ac in l.mArchiveList) {
				if (ac.ArchiveType != ArchiveControl.ArchiveTypeEnum.Column)
					continue;

				ArchivedData ad = new ArchivedData(ac, id);
				if (ad.mData != null)
					return ad;
			}

			return null;
		}


		public Dictionary<string, object> Read() {
			mData = null;

			if (mArchiveControl.IsCompressed) {
				byte[] b = Compression.DeCompress(mArchiveControl.ArchiveMethod.Read(mID));
				if (b == null)
					return null;

				mData = (Dictionary<string, object>)DataUtils.DeserializeObj(b);
			} else {
				mData = mArchiveControl.ArchiveMethod.ReadHash(mID);
			}

			return mData;

			//i was thinking for parent/child tables, if a read is done on the parnet, we automaticly take 
			//the parent and child record (that are prpbablyin the archived row) and write them out to the table so they will be available 
			//we can leave the archive record so the archiver only needs to delete the records from the primary tables next time it runs
			//or delete it from the archive and let the archiver rearchive it - race conditions?
		}


		public void Delete() {
			if (mArchiveControl.IsCompressed)
				mArchiveControl.ArchiveMethod.Delete(mID);
			else {
				mArchiveControl.ArchiveMethod.DeleteHash(mID);
			}
		}


		public static string DisplayArchivedData(string TablePath, long id) {
			string ret = "";

			ArchivedData ad = ArchivedData.ReadRecord(TablePath, id);
			if (ad == null)
				ret += "no row archive found\n";
			else {
				ret += "Row archive found " + ad.mArchiveControl.ArchiveInfo + "\n";
				foreach (KeyValuePair<string, object> kvp in new SortedList<string, object>(ad.Data)) {
					ret += kvp.Key.ToString() + ":" + kvp.Value.ToString() + "\n";
				}
			}

			ret += "\n\n";

			ad = ArchivedData.ScanAllColumnArchives(TablePath, id);
			if (ad == null)
				ret += "no column archive found\n";
			else {
				ret += "Column archive found " + ad.mArchiveControl.ArchiveInfo + "\n";
				foreach (KeyValuePair<string, object> kvp in new SortedList<string, object>(ad.Data)) {
					ret += kvp.Key.ToString() + ":" + kvp.Value.ToString() + "\n";
				}
			}

			return ret;
		}
	}


	/// <summary>
	/// class for compressing data
	/// </summary>
	public sealed class Compression {

		public static byte[] Compress(string strInput) {
			if (strInput == null)
				return null;

			return Compress(System.Text.Encoding.UTF8.GetBytes(strInput));
		}

		public static byte[] Compress(byte[] bytData) {
			MemoryStream ms = new MemoryStream();
			Stream s = new DeflaterOutputStream(ms);
			s.Write(bytData, 0, bytData.Length);
			s.Close();
			byte[] compressedData = (byte[])ms.ToArray();
			return compressedData;
		}


		public static string DeCompressToString(byte[] bytInput) {
			if (bytInput == null || bytInput.Length == 0)
				return null;

			string strResult = "";
			int totalLength = 0;
			byte[] writeData = new byte[10240];
			Stream s2 = new InflaterInputStream(new MemoryStream(bytInput));

			while (true) {
				int size = s2.Read(writeData, 0, writeData.Length);
				if (size > 0) {
					totalLength += size;
					strResult += System.Text.Encoding.ASCII.GetString(writeData, 0,
						size);
				} else {
					break;
				}
			}

			s2.Close();
			return strResult;

		}

		public static byte[] DeCompress(byte[] bytInput) {
			if (bytInput == null || bytInput.Length == 0)
				return null;

			Stream s2 = new InflaterInputStream(new MemoryStream(bytInput));
			MemoryStream mem = new MemoryStream();
			byte[] writeData = new byte[10240];
			try {
				while (true) {
					int size = s2.Read(writeData, 0, writeData.Length);
					if (size > 0) {
						mem.Write(writeData, 0, size);
					} else {
						break;
					}
				}

				mem.Close();
				return mem.ToArray();
			} finally {
				s2.Close();
			}
		}
	}
}
