using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.SessionState;

namespace SQLinqenlot {
	/// <summary>
	/// UniStatic is a class that stores any kind of value, and works whether in desktop or web apps, by storing the value in
	/// the Session object if this is a web app.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class UniStatic<T> {
		private enum TMode {
			Desktop,
			Web,
		}
		private static TMode? _Mode;
		private static TMode Mode {
			get {
				if (_Mode == null) {
					if (HttpContext.Current == null)
						_Mode = TMode.Desktop;
					else
						_Mode = TMode.Web;
				}
				return _Mode.Value;
			}
		}

		private static HttpSessionState Session {
			get { return HttpContext.Current.Session; }
		}

		public UniStatic() { }
		public UniStatic(T value) {
			this.Value = value;
		}

		private T _Value;
		private Guid _Guid = Guid.NewGuid();

		public T Value {
			get {
				switch (Mode) {
					case TMode.Web:
						try {
							return (T)Session[_Guid.ToString()];
						} catch (NullReferenceException) {
							return default(T);
						}
					case TMode.Desktop:
						return _Value;
					default:
						throw new EnumerationValueException(Mode);
				}
			}
			set {
				switch (Mode) {
					case TMode.Web:
						Session[_Guid.ToString()] = value;
						break;
					case TMode.Desktop:
						_Value = value;
						break;
				}
			}
		}

		public override string ToString() {
			if (Value == null)
				return null;
			return Value.ToString();
		}

		public static bool operator ==(UniStatic<T> obj1, UniStatic<T> obj2) {
			if ((object)obj1 == null && (object)obj2 == null)
				return true;
			if ((object)obj1 == null ^ (object)obj2 == null)
				return false;
			if (obj1.Value == null && obj2.Value == null)
				return true;
			if (obj1.Value == null ^ obj2.Value == null)
				return false;
			return obj1.Value.Equals(obj2.Value);
		}
		public static bool operator !=(UniStatic<T> obj1, UniStatic<T> obj2) {
			return (!(obj1 == obj2));
		}
		public override bool Equals(object obj) {
			var obj2 = obj as UniStatic<T>;
			return this == obj2;
		}
		public override int GetHashCode() {
			return base.GetHashCode();
		}
	}
}
