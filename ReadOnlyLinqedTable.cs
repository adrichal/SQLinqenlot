using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLinqenlot {
	public class ReadOnlyLinqedTable<T> : LinqedTable<T> where T : LinqedTable<T> {
		public override void SubmitChanges() {
			throw new NotSupportedException("Cannot make changes to the read-only object " + GetType().Name);
		}
		public override void InsertOnSubmit() {
			throw new NotSupportedException("Cannot make changes to the read-only object " + GetType().Name);
		}
		public override void DeleteOnSubmit() {
			throw new NotSupportedException("Cannot make changes to the read-only object " + GetType().Name);
		}
	}
}
