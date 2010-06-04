using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace SQLinqenlot.Archiving {
	/// <summary>
	/// Summary description for FileSystemArchiveMethod.
	/// </summary>
	public class FileSystemArchiveMethod : ArchiveMethod {


		public FileSystemArchiveMethod(ArchiveControl ctl) : base(ctl) {}

		public override void Write(long id, byte [] data) {
			string path = BuildFilePath(id, true);
			this.Delete(id);		

			System.IO.FileStream fs = null;
			try {
				fs = new System.IO.FileStream(path, System.IO.FileMode.Create,System.IO.FileAccess.Write);
				fs.Write(data,0,data.Length);
			}
			finally {
				if (fs !=null)
					fs.Close();
			}
		}

		public override  byte[] Read (long ID) {
			System.IO.FileStream fs = null;
			System.IO.BinaryReader	r = null;
			try {
				string path = BuildFilePath(ID);
				if (!System.IO.File.Exists(path))
					return null;

				fs = new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read);
				r = new System.IO.BinaryReader(fs);

				int len = (int)fs.Length;
				byte [] b = r.ReadBytes(len);
		
				return b;
			}
			finally {
				if (r != null)
					r.Close();
			
				if (fs != null)
					fs.Close();
			}
		}

		public override int Delete(long ID) {
			string path = BuildFilePath(ID);
			System.IO.FileInfo fi = new System.IO.FileInfo(path);
			if (fi.Exists) {
				fi.Delete();
				return 1;
			}

			return 0;
		}
		

		private string BuildFilePath(long id) {
			return BuildFilePath(id, true);
		}

		private string BuildFilePath(long id, bool CreateDirs) {
			string idpath = id.ToString().PadLeft(11,'0');
			int len = idpath.Length;
			string part3 = idpath.Substring(len-4,4);
			string part2 = idpath.Substring(len-8,4);
			string part1 = idpath.Substring(0, len-8);

			if (!CreateDirs)
				return String.Format("({0}\\{1}\\{2}\\{3}\\{4}", mArchiveControl.ArchiveInfo, mArchiveControl.ID, part1, part2, id);

			string path = mArchiveControl.ArchiveInfo;
			foreach (string n in new string [] {mArchiveControl.ID.ToString(), part1, part2}) {
				path += "\\" + n;
				if (!System.IO.File.Exists(path)) 
					System.IO.Directory.CreateDirectory(path);
			}
			
			return path + "\\" + id.ToString();
		}
	}
}
