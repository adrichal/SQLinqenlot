using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SQLinqenlot.Archiving {
	/// <summary>
	/// interface that an archive method (like sql or filesystem) has to follow that will allow us to create, read, and delete from
	/// an actual archive
	/// </summary>
	public abstract class ArchiveMethod {

		protected ArchiveControl mArchiveControl;
		public ArchiveMethod(ArchiveControl ctl) {
			mArchiveControl = ctl;
		}

		public virtual Dictionary<string, object> ReadHash(long ID) {
			throw new Exception("Read hash not implemented");
		}

		public abstract byte [] Read (long ID);


		public virtual void WriteHash(long ID, Dictionary<string, object> data) {
			throw new Exception("Write hash not implemented");
		}

		public abstract void Write(long id, byte [] CompressedData);

		public abstract int Delete(long ID); //returns 1 on successes or 0 if not found

		public virtual int DeleteHash(long ID) {
			throw new Exception("Delete hash not implemented");
		}
	}
}
