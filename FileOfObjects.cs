using System;
using System.IO;

namespace SQLinqenlot {
	public class FileOfObjects {

		public byte[] RawObject;
		public string AssemblyName;

		System.IO.BinaryReader r;
		FileStream fs;
		public FileOfObjects(string FileName) {
			fs = new FileStream(FileName, System.IO.FileMode.Open, System.IO.FileAccess.Read);
			r = new BinaryReader(fs);
		}

		public bool ReadNext() {
			if (r.BaseStream.Position == r.BaseStream.Length)
				return false;

			AssemblyName = r.ReadString();
			int len = r.ReadInt32();
			RawObject = r.ReadBytes(len);

			return true;
		}

		public void Close() {
			r.Close();
			fs.Close();
		}

		public static bool Write(object o, string FileName) {
			Type t = o.GetType();

			byte[] rec = DataUtils.SerializeObj(o);

			try {
				FileStream fs = new FileStream((FileName), System.IO.FileMode.Append, System.IO.FileAccess.Write);

				GetExclusiveAccess(fs);

				System.IO.BinaryWriter w = new BinaryWriter(fs);

				//save assembly - so we can load in the the recoverer
				w.Write(t.Assembly.FullName);
				w.Write(rec.Length);
				w.Write(rec);

				w.Close();
				fs.Close();
				return true;
			} catch {
				return false;
			}
		}

		private static void GetExclusiveAccess(FileStream fs) {
			int i;

			for (i = 0; i < 10; i++) {
				try {
					fs.Lock(0, 1);
					return;
				} catch {
					System.Threading.Thread.Sleep(2000);
				}
			}

			throw new Exception("cant get lock for sql recovery on file " + fs.Name);
		}
	}
}
