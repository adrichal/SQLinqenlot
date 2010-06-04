using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Linq;
using System.Reflection;

namespace SQLinqenlot {
	public abstract class AuditHeaderBase : LinqedTable {
		#region Constructors
		public AuditHeaderBase() {
		}
		#endregion

		#region Reflection/Expected fields
		internal protected virtual string MyDataType {
			get { return ReflectionUtils.GetReflectedProperty(this, "DataType").ToString(); }
			set { ReflectionUtils.SetReflectedProperty(this, "DataType", value); }
		}
		internal protected virtual string MyEventDescription {
			get { return ReflectionUtils.GetReflectedProperty(this, "EventDescription").ToString(); }
			set { ReflectionUtils.SetReflectedProperty(this, "EventDescription", value); }
		}
		internal protected virtual string MyCompletionMsg {
			get { return ReflectionUtils.GetReflectedProperty(this, "CompletionMsg").ToString(); }
			set { ReflectionUtils.SetReflectedProperty(this, "CompletionMsg", value); }
		}
		internal protected virtual long MyDataID {
			get { return (long)ReflectionUtils.GetReflectedProperty(this, "DataID"); }
			set { ReflectionUtils.SetReflectedProperty(this, "DataID", value); }
		}
		internal protected virtual short MyCompletionStatus {
			get { return (short)ReflectionUtils.GetReflectedProperty(this, "CompletionStatus"); }
			set { ReflectionUtils.SetReflectedProperty(this, "CompletionStatus", value); }
		}
		internal protected virtual DateTime MyLogTime {
			get { return (DateTime)ReflectionUtils.GetReflectedProperty(this, "LogTime"); }
			set { ReflectionUtils.SetReflectedProperty(this, "LogTime", value); }
		}
		internal protected virtual Type AuditDetailType {
			get {
				PropertyInfo AuditDetailsProperty = GetType().GetProperty("AuditDetails");
				// this is expected to be a generic "EntitySet" type property
				if (!AuditDetailsProperty.PropertyType.IsGenericType)
					throw new Exception("AuditDetails property is expected to be generic.");
				return AuditDetailsProperty.PropertyType.GetGenericTypeDefinition().GetGenericArguments()[0];
			}
		}
		protected sealed internal override bool AutoAudit {
			get { return false; }
		}
		#endregion

		#region Data persistence
		protected internal override void BeforeInsert() {
			MyLogTime = DateTimeUtility.ServerDate();
		}
		#endregion
	}
}
