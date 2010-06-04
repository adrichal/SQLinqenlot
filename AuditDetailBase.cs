using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLinqenlot {
	public abstract class AuditDetailBase : LinqedTable {
		#region Expected fields
		internal protected virtual AuditHeaderBase MyAuditHeader {
			get { return (AuditHeaderBase)ReflectionUtils.GetReflectedProperty(this, "AuditHeader"); }
			set { ReflectionUtils.SetReflectedProperty(this, "AuditHeader", value); }
		}
		internal protected virtual string MyField {
			get { return (string)ReflectionUtils.GetReflectedProperty(this, "Field"); }
			set { ReflectionUtils.SetReflectedProperty(this, "Field", value); }
		}
		internal protected virtual string MyOldValue {
			get { return (string)ReflectionUtils.GetReflectedProperty(this, "OldValue"); }
			set { ReflectionUtils.SetReflectedProperty(this, "OldValue", value); }
		}
		internal protected virtual string MyNewValue {
			get { return (string)ReflectionUtils.GetReflectedProperty(this, "NewValue"); }
			set { ReflectionUtils.SetReflectedProperty(this, "NewValue", value); }
		}
		protected sealed internal override bool AutoAudit {
			get { return false; }
		}
		#endregion
	}
}
