using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLinqenlot {
	public class DataValidationException : Exception {
		public DataValidationException() : base() { }

		public DataValidationException(string Message) : base(Message) { }

		public DataValidationException(string Message, Exception InnerException) : base(Message, InnerException) { }
	}

	public class EnumerationValueException : Exception {
		public EnumerationValueException(Enum value) :
			base(string.Format("Value {0} of enumeration {1} is not supported.", value, value.GetType().Name)) { }
	}
}
