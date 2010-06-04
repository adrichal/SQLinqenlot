using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;
using System.Threading;
using Microsoft.Win32;
using System.Reflection;
using ICSharpCode.SharpZipLib.Zip;
using System.Diagnostics;
using iTextSharp.text;
using iTextSharp.text.pdf;
using System.Runtime.Serialization.Formatters.Binary;
using System.Data.Linq;
using System.Security.Cryptography;
using System.Web.Security;
using System.Net.Mail;

namespace SQLinqenlot {
	public static class DataUtils {
		/// <summary>
		/// Returns the string value of o, or a blank string if o is null or DBNull.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static string BlankIfNull(this object o) {
			if (o == null || o.Equals(DBNull.Value))
				return "";
			return o.ToString();
		}
		/// <summary>
		/// Returns the value of o, or zero if o is null or DBNull.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static object ZeroIfNull(this object o) {
			if (o == null || o.Equals(DBNull.Value))
				return 0;
			return o;
		}

		public static long LongZeroIfNull(object o) {
			try {
				// C# can be very stupid about converting DB types from, say, Int32 to long.  So we have to do a 2 step conversion to ensure we don't get any errors.
				return (long)o;
			} catch {
				return 0;
			}
		}

		public static object DBNullIfNull(object Value) {
			if (Value == null) {
				return DBNull.Value;
			} else if (Value is DateTime && ((DateTime)Value).Ticks == 0) {
				return DBNull.Value;
			} else {
				return Value;
			}
		}

		#region Safe type casting
		/// <summary>
		/// Returns a long value, or zero if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static long ToLong(this object o) {
			return ToNullableLong(o).GetValueOrDefault(0);
		}
		/// <summary>
		/// Returns an int value, or zero if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static int ToInt(this object o) {
			return ToNullableInt(o).GetValueOrDefault(0);
		}
		/// <summary>
		/// Returns a short value, or zero if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static short ToShort(this object o) {
			return ToNullableShort(o).GetValueOrDefault(0);
		}
		/// <summary>
		/// Returns a byte value, or zero if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static byte ToByte(this object o) {
			return ToNullableByte(o).GetValueOrDefault(0);
		}
		/// <summary>
		/// Returns a bool value, or false if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static bool ToBool(this object o) {
			return ToNullableBool(o).GetValueOrDefault(false);
		}
		/// <summary>
		/// Returns a decimal value, or zero if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static decimal ToDecimal(this object o) {
			return ToNullableDecimal(o).GetValueOrDefault(0);
		}
		/// <summary>
		/// Returns a float value, or zero if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static float ToSingle(this object o) {
			return ToNullableSingle(o).GetValueOrDefault(0);
		}
		/// <summary>
		/// Returns a double value, or zero if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static double ToDouble(this object o) {
			return ToNullableDouble(o).GetValueOrDefault(0);
		}
		/// <summary>
		/// Returns a date value, or null if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static DateTime? ToDate(this object o) {
			try {
				if (o is string)
					return DateTime.Parse((string)o);
				return (DateTime)o;
			} catch {
				if (o is string) {
					string s = (string)o;
					if (s.Length == 6) {
						s = s.Substring(0, 2) + "/" + s.Substring(2, 2) + "/" + s.Substring(4);
						return ToDate(s);
					} else if (s.Length == 8) {
						string s1 = s.Substring(0, 2) + "/" + s.Substring(2, 2) + "/" + s.Substring(4);
						string s2 = s.Substring(0, 4) + "/" + s.Substring(4, 2) + "/" + s.Substring(6);
						DateTime? Result = ToDate(s1);
						if (Result == null) {
							Result = ToDate(s2);
						}
						return Result;
					}
				}
				return null;
			}
		}
		/// <summary>
		/// Returns a long value, or null if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static long? ToNullableLong(this object o) {
			return (long?)ToNullableDouble(o);
		}
		/// <summary>
		/// Returns an int value, or null if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static int? ToNullableInt(this object o) {
			return (int?)ToNullableDouble(o);
		}
		/// <summary>
		/// Returns a short value, or null if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static short? ToNullableShort(this object o) {
			return (short?)ToNullableDouble(o);
		}
		/// <summary>
		/// Returns a byte value, or null if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static byte? ToNullableByte(this object o) {
			return (byte?)ToNullableDouble(o);
		}
		/// <summary>
		/// Returns a bool value, or null if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static bool? ToNullableBool(this object o) {
			try {
				if (o == null)
					return null;
				if (o is string)
					return bool.Parse((string)o);
				return Convert.ToBoolean(o);
			} catch {
				return null;
			}
		}
		/// <summary>
		/// Returns a decimal value, or null if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static decimal? ToNullableDecimal(this object o) {
			return (decimal?)ToNullableDouble(o);
		}
		/// <summary>
		/// Returns a float value, or null if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static float? ToNullableSingle(this object o) {
			return (float?)ToNullableDouble(o);
		}
		/// <summary>
		/// Returns a double value, or null if the type casting failed.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static double? ToNullableDouble(this object o) {
			try {
				if (o == null)
					return null;
				if (o is string)
					return double.Parse((string)o);
				return Convert.ToDouble(o);
			} catch {
				return null;
			}
		}
		/// <summary>
		/// Returns null if o is null, or o.ToString() if not.
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public static string ToNullableString(this object o) {
			if (o == null)
				return null;
			else
				return o.ToString();
		}

		public static T ToEnum<T>(this object o) where T : struct, IConvertible {
			return ToNullableEnum<T>(o).GetValueOrDefault();
		}

		public static T? ToNullableEnum<T>(this object o) where T : struct, IConvertible {
			if (!typeof(T).IsEnum)
				throw new Exception(string.Format("Type {0} is not an enumeration.", typeof(T)));
			if (o == null)
				return null;
			try {
				if (o is string)
					return (T)Enum.Parse(typeof(T), o.ToString());
				return (T)o;
			} catch {
				return null;
			}
		}

		public static bool? ValueToBool(this string value) {
			switch (value.ToLower()) {
				case "true":
					return true;
				case "false":
					return false;
				case "":
					return null;
				default:
					return null;
			}
		}

		public static string IfNullDefaultTo(this string value, string defaultValue) {
			if (value == null)
				return defaultValue;
			else
				return value;
		}
		#endregion

		/// <summary>
		/// Converts an enumeration type to a set of values keyed by Value and Description, and ordered by description.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public static object EnumToDataSource<T>() where T : struct, IConvertible {
			return EnumToDataSource<T>(false, null);
		}
		/// <summary>
		/// Converts an enumeration type to a set of values keyed by Value and Description.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public static object EnumToDataSource<T>(bool OrderByValue) where T : struct, IConvertible {
			return EnumToDataSource<T>(OrderByValue, null);
		}
		/// <summary>
		/// Converts an enumeration type to a set of values keyed by Value and Description.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public static object EnumToDataSource<T>(bool OrderByValue, byte[] ExcludeValues) where T : struct, IConvertible {
			if (!typeof(T).IsEnum)
				throw new Exception(string.Format("Type {0} is not an enumeration.", typeof(T)));
			var q = Enum.GetValues(typeof(T)).Cast<T>()
				.Select(v => new { Value = DataUtils.ToByte(v), Description = TextUtility.SpaceAtCapitals(v.ToString()) });
			if (ExcludeValues != null)
				q = q.Where(v => !v.Value.In(ExcludeValues));
			if (OrderByValue)
				q = q.OrderBy(v => v.Value);
			else
				q = q.OrderBy(v => v.Description);
			return q;
		}

		public static byte[] SerializeObj(object o) {
			if ((o == null)) return null;

			MemoryStream s = new MemoryStream();
			BinaryFormatter bf = new BinaryFormatter();
			bf.Serialize(s, o);
			s.Close();
			return s.GetBuffer();
		}

		public static object DeserializeObj(byte[] b) {
			if ((b == null)) return null;

			object o = null;
			MemoryStream s = new MemoryStream(b);
			BinaryFormatter bf = new BinaryFormatter();
			try {
				o = bf.Deserialize(s);
			} catch {
				o = null;
			}
			s.Close();
			return o;
		}

		public static DateTime? NullIfBlank(this DateTime dateTime) {
			if (dateTime == DateTime.MinValue)
				return null;
			return dateTime;
		}
		public static string NullIfBlank(this string value) {
			if (value == "")
				return null;
			return value;
		}
		public static byte? NullIfZero(this byte value) {
			if (value == 0)
				return null;
			return value;
		}
		public static short? NullIfZero(this short value) {
			if (value == 0)
				return null;
			return value;
		}
		public static int? NullIfZero(this int value) {
			if (value == 0)
				return null;
			return value;
		}
		public static long? NullIfZero(this long value) {
			if (value == 0)
				return null;
			return value;
		}
		public static float? NullIfZero(this float value) {
			if (value == 0)
				return null;
			return value;
		}
		public static double? NullIfZero(this double value) {
			if (value == 0)
				return null;
			return value;
		}
		public static decimal? NullIfZero(this decimal value) {
			if (value == 0)
				return null;
			return value;
		}
	}

	public static class LinqUtils {
		static LinqUtils() {
		}

		private static UniStatic<Dictionary<long, Dictionary<TDatabase, LinqedTableDataContext>>> __ClientDataContexts = new UniStatic<Dictionary<long, Dictionary<TDatabase, LinqedTableDataContext>>>();
		private static Dictionary<long, Dictionary<TDatabase, LinqedTableDataContext>> ClientDataContexts {
			get {
				if (__ClientDataContexts.Value == null)
					__ClientDataContexts.Value = new Dictionary<long, Dictionary<TDatabase, LinqedTableDataContext>>();
				return __ClientDataContexts.Value;
			}
		}
		private static UniStatic<Dictionary<Type, Type>> __DataContextTypes = new UniStatic<Dictionary<Type, Type>>();
		private static Dictionary<Type, Type> DataContextTypes {
			get {
				if (__DataContextTypes.Value == null)
					__DataContextTypes.Value = new Dictionary<Type, Type>();
				return __DataContextTypes.Value;
			}
		}
		private static UniStatic<Dictionary<Type, bool>> __AlreadyRegistered = new UniStatic<Dictionary<Type, bool>>();
		private static Dictionary<Type, bool> AlreadyRegistered {
			get {
				if (__AlreadyRegistered.Value == null)
					__AlreadyRegistered.Value = new Dictionary<Type, bool>();
				return __AlreadyRegistered.Value;
			}
		}

		public static void RegisterDataContext(LinqedTableDataContext context) {
			Type contextType = context.GetType();
			if (AlreadyRegistered.ContainsKey(contextType))
				return;
			// iterate through the Get<> methods and register the underlying types with this context
			PropertyInfo[] props = contextType.GetProperties();
			foreach (PropertyInfo prop in props) {
				if (!prop.PropertyType.IsGenericType)
					continue;
				Type GenericType = prop.PropertyType.GetGenericTypeDefinition();
				if (typeof(Table<>) != GenericType)
					continue;
				// now, what type does it contain?
				Type ContainedType = prop.PropertyType.GetGenericArguments()[0];
				if (!typeof(LinqedTable).IsAssignableFrom(ContainedType))
					continue;
				DataContextTypes[ContainedType] = contextType;
			}
			AlreadyRegistered[contextType] = true;
		}
		/// <summary>
		/// Returns the type of data context that contains the specified type.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <returns></returns>
		public static Type GetDataContextType<T>() where T : LinqedTable {
			return GetDataContextType(typeof(T));
		}
		/// <summary>
		/// Returns the type of data context that contains the specified type.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static Type GetDataContextType(Type type) {
			if (!typeof(LinqedTable).IsAssignableFrom(type))
				throw new ArgumentException("Type must inherit from LinqedTable", "type");
			return DataContextTypes[type];
		}
		/// <summary>
		/// Returns a data context that contains the specified class, using the active client configuration.
		/// </summary>
		/// <typeparam name="T">The class for which a data context is required.</typeparam>
		/// <returns></returns>
		public static LinqedTableDataContext GetDataContext<T>() where T : LinqedTable {
			return GetDataContext<T>(DBLocator.ActiveClientID);
		}
		/// <summary>
		/// Returns a data context that contains the specified class with a connection appropriate to the specified client.
		/// </summary>
		/// <typeparam name="T">The class for which a data context is required.</typeparam>
		/// <param name="ClientID"></param>
		/// <returns></returns>
		public static LinqedTableDataContext GetDataContext<T>(long ClientID) where T : LinqedTable {
			return GetDataContext(typeof(T), ClientID);
		}
		/// <summary>
		/// Returns a data context that contains the specified class, using the active client configuration.
		/// </summary>
		/// <param name="type">The class for which a data context is required.</param>
		/// <returns></returns>
		public static LinqedTableDataContext GetDataContext(Type type) {
			return GetDataContext(type, DBLocator.ActiveClientID);
		}
		/// <summary>
		/// Returns a data context that contains the specified class, using the active client configuration.
		/// </summary>
		/// <param name="type">The class for which a data context is required.</param>
		/// <returns></returns>
		public static LinqedTableDataContext GetDataContext(Type type, long ClientID) {
			if (!typeof(LinqedTable).IsAssignableFrom(type))
				throw new ArgumentException("Type must inherit from LinqedTable", "type");
			if (!ClientDataContexts.ContainsKey(ClientID))
				ClientDataContexts[ClientID] = new Dictionary<TDatabase, LinqedTableDataContext>();
			LinqedTable instance = (LinqedTable)Activator.CreateInstance(type);
			if (!ClientDataContexts[ClientID].ContainsKey(instance.Database)) {
				// create a data context for the specified database
				string DBName = instance.Database.ToString();
				string ServerName;
				long CurrentClientID = DBLocator.ActiveClientID;
				SyntacClient client;
				try {
					DBLocator.ActiveClientID = ClientID;
					ServerName = DBLocator.getDatabaseServer(ref DBName);
					client = DBLocator.ActiveClient;
				} finally {
					DBLocator.ActiveClientID = CurrentClientID;
				}
				LinqedTableDataContext context = (LinqedTableDataContext)Activator.CreateInstance(DataContextTypes[type]);
				string NewConnectionString = "Data Source={0};Initial Catalog={1};User ID={2};Password={3};Max Pool Size=100"
					.Fmt(ServerName, DBName, client.ReadWriteLogin, client.ReadWritePassword);
				context.Connection.ConnectionString = NewConnectionString;
				ClientDataContexts[ClientID][instance.Database] = context;
			}
			return ClientDataContexts[ClientID][instance.Database];
		}
		/// <summary>
		/// Purges the cache of data contexts.
		/// </summary>
		public static void PurgeDataContexts() {
			ClientDataContexts.Clear();
		}

		public static Table<T> GetTable<T>() where T : LinqedTable {
			return GetTable<T>(DBLocator.ActiveClientID);
		}

		public static Table<T> GetTable<T>(long ClientID) where T : LinqedTable {
			return (Table<T>)GetTable(typeof(T), ClientID);
		}

		public static ITable GetTable(Type type) {
			return GetTable(type, DBLocator.ActiveClientID);
		}

		public static ITable GetTable(Type type, long ClientID) {
			return GetDataContext(type, ClientID).GetTable(type);
		}
	}
	/// <summary>
	/// Utility class for reflection
	/// </summary>
	public static class ReflectionUtils {
		public static object GetReflectedProperty(object obj, string PropertyName) {
			return obj.GetType().GetProperty(PropertyName).GetValue(obj, null);
		}

		public static void SetReflectedProperty(object obj, string PropertyName, object value) {
			obj.GetType().GetProperty(PropertyName).SetValue(obj, value, null);
		}
	}
	/// <summary>
	/// Utility class for date/time functions
	/// </summary>
	public static class DateTimeUtility {
		static DateTimeUtility() {
			__TimeDiff = TimeSpan.MinValue;
			__NextTimeCheck = DateTime.MinValue;
			__SynchLock = new object();
		}
		private static TimeSpan __TimeDiff;
		private static DateTime __NextTimeCheck;
		private const int mnFRESHNESS_MINUTES = 10;
		private static object __SynchLock;
		/// <summary>
		/// Returns the current time on the database server.
		/// </summary>
		/// <remarks>This only makes one call to the server per 10 minutes;
		/// thereafter the time difference between the current machine and the server is cached.
		/// </remarks>
		/// <param name="ForceSync">If force sync is on, it will go out to the server to get the current date</param>
		/// <returns></returns>
		public static DateTime ServerDate(bool ForceSync) {
			lock (__SynchLock) {
				if (DateTime.Now > __NextTimeCheck || ForceSync) {
					// time to refresh
					try {
						SqlUtil sql = new SqlUtil(TDatabase.Shared);
						DateTime dServerDate = DataUtils.ToDate(sql.ExecuteScalarResultSetSQLQuery("select getdate()")).Value;
						__TimeDiff = DateTime.Now.Subtract(dServerDate);
						__NextTimeCheck = DateTime.Now.AddMinutes(mnFRESHNESS_MINUTES);
					} catch {
						if (__TimeDiff == TimeSpan.MinValue)
							__TimeDiff = DateTime.Now.Subtract(DateTime.Now);
					}
				}

				return DateTime.Now.Subtract(__TimeDiff);
			}
		}

		public static DateTime ServerDateNoRefresh() {
			return DateTime.Now.Subtract(__TimeDiff);
		}

		public static DateTime ServerDate() {
			return ServerDate(false);
		}

		public static object DateNullIfVoid(System.DateTime TheDate) {
			if (TheDate.Ticks == 0) {
				return null;
			} else {
				return TheDate;
			}
		}

		public static System.DateTime DateVoidIfNull(object TheDate) {
			if (TheDate == DBNull.Value) {
				return DateTime.MinValue;
			} else {
				return (DateTime)TheDate;
			}
		}

		public static object TimeNullIfVoid(TimeSpan TheTime) {
			if (TheTime.Ticks == 0) {
				return DBNull.Value;
			} else {
				return TheTime;
			}
		}

		public static TimeSpan TimeVoidIfNull(object TheTime) {
			if (TheTime == null || TheTime == DBNull.Value) {
				return TimeSpan.MinValue;
			} else {
				return ((DateTime)TheTime).TimeOfDay;
			}
		}
		/// <summary>
		/// Creates a DateTime with a "safe" day of month, i.e. if it's too high, it will return the last day of the month
		/// </summary>
		/// <param name="Year"></param>
		/// <param name="Month"></param>
		/// <param name="Day"></param>
		/// <returns></returns>
		public static DateTime SafeGetDate(int Year, int Month, int Day) {
			return SafeGetDate(Year, Month, Day, 0, 0, 0, 0);
		}
		/// <summary>
		/// Creates a DateTime with a "safe" day of month, i.e. if it's too high, it will return the last day of the month
		/// </summary>
		/// <param name="Year"></param>
		/// <param name="Month"></param>
		/// <param name="Day"></param>
		/// <param name="Hour"></param>
		/// <param name="Minute"></param>
		/// <param name="Second"></param>
		/// <returns></returns>
		public static DateTime SafeGetDate(int Year, int Month, int Day, int Hour, int Minute, int Second) {
			return SafeGetDate(Year, Month, Day, Hour, Minute, Second, 0);
		}
		/// <summary>
		/// Creates a DateTime with a "safe" day of month, i.e. if it's too high, it will return the last day of the month
		/// </summary>
		/// <param name="Year"></param>
		/// <param name="Month"></param>
		/// <param name="Day"></param>
		/// <param name="Hour"></param>
		/// <param name="Minute"></param>
		/// <param name="Second"></param>
		/// <param name="Millisecond"></param>
		/// <returns></returns>
		public static DateTime SafeGetDate(int Year, int Month, int Day, int Hour, int Minute, int Second, int Millisecond) {
			if (DateTime.DaysInMonth(Year, Month) < Day)
				Day = DateTime.DaysInMonth(Year, Month);
			return new DateTime(Year, Month, Day, Hour, Minute, Second, Millisecond);
		}
		/// <summary>
		/// Returns a date corresponding to the nth x-day in the given month.
		/// </summary>
		/// <param name="Year"></param>
		/// <param name="Month"></param>
		/// <param name="wt"></param>
		/// <param name="WeekdayOrdinal">Positive numbers to count from the beginning of the month; negative numbers to count backwards.</param>
		/// <returns></returns>
		/// <remarks>Absolute value of WeekOfMonth must be between 1 and 4.  If you want the 5th occurrence of a weekday in a month, supply -1, which
		/// will return the last occurrence.</remarks>
		public static DateTime GetDate(int Year, int Month, TWeekdayType wt, int WeekdayOrdinal) {
			// range checking
			if (Math.Abs(WeekdayOrdinal) < 1 || Math.Abs(WeekdayOrdinal) > 4)
				throw new ArgumentException("Absolute value of week of month must be between 1 and 4.", "WeekOfMonth");
			if (WeekdayOrdinal > 0) {
				DateTime day = new DateTime(Year, Month, 1);
				while (!DayMatchesWeekdayType(day, wt))
					day = day.AddDays(1);
				return day.AddDays(7 * (WeekdayOrdinal - 1));
			} else { // work from end of month
				DateTime day = SafeGetDate(Year, Month, 31);
				while (!DayMatchesWeekdayType(day, wt))
					day = day.AddDays(-1);
				return day.AddDays(7 * (WeekdayOrdinal + 1));
			}
		}

		public static bool DayMatchesWeekdayType(this DateTime dt, TWeekdayType wt) {
			if (wt == TWeekdayType.Day)
				return true;
			if ((int)dt.DayOfWeek == (int)wt)
				return true;
			switch (dt.DayOfWeek) {
				case DayOfWeek.Saturday:
				case DayOfWeek.Sunday:
					return wt == TWeekdayType.WeekendDay;
				default:
					return wt == TWeekdayType.Weekday;
			}
		}
		/// <summary>
		/// Returns the server date without the time component.
		/// </summary>
		/// <returns></returns>
		public static DateTime Today() {
			DateTime dt = ServerDate();
			return new DateTime(dt.Year, dt.Month, dt.Day);
		}
		/// <summary>
		/// Returns the next occurrence of the specified day of the week.
		/// </summary>
		/// <param name="dw"></param>
		/// <returns></returns>
		public static DateTime GetNextOccurrenceOf(this DayOfWeek dw) {
			return GetNextOccurrenceOf(dw, Today());
		}
		/// <summary>
		/// Returns the next occurrence of the specified day of the week from the given start date.
		/// </summary>
		/// <param name="dw"></param>
		/// <param name="StartDate"></param>
		/// <returns></returns>
		public static DateTime GetNextOccurrenceOf(this DayOfWeek dw, DateTime StartDate) {
			DateTime dt = StartDate;
			while (dt.DayOfWeek != dw)
				dt = dt.AddDays(1);
			return dt;
		}
		/// <summary>
		/// Returns the previous occurrence of the specified day of the week.
		/// </summary>
		/// <param name="dw"></param>
		/// <returns></returns>
		public static DateTime GetPreviousOccurrenceOf(this DayOfWeek dw) {
			return GetPreviousOccurrenceOf(dw, Today());
		}
		/// <summary>
		/// Returns the previous occurrence of the specified day of the week from the given start date.
		/// </summary>
		/// <param name="dw"></param>
		/// <param name="StartDate"></param>
		/// <returns></returns>
		public static DateTime GetPreviousOccurrenceOf(this DayOfWeek dw, DateTime StartDate) {
			DateTime dt = StartDate;
			while (dt.DayOfWeek != dw)
				dt = dt.AddDays(-1);
			return dt;
		}
		/// <summary>
		/// Returns all dates between start and end (exclusive of end date).
		/// </summary>
		/// <param name="start"></param>
		/// <param name="end"></param>
		/// <returns></returns>
		public static IEnumerable<DateTime> GetDaysInPeriod(DateTime start, DateTime end) {
			return GetDaysInPeriod(start, end, Enum.GetValues(typeof(DayOfWeek)).Cast<DayOfWeek>());
		}
		/// <summary>
		/// Returns all dates between start and end (exclusive of end date) that match the specified days of the week.
		/// </summary>
		/// <param name="start"></param>
		/// <param name="end"></param>
		/// <param name="daysOfWeek"></param>
		/// <returns></returns>
		public static IEnumerable<DateTime> GetDaysInPeriod(DateTime start, DateTime end, IEnumerable<DayOfWeek> daysOfWeek) {
			DateTime dt = start;
			while (dt < end) {
				if (daysOfWeek.Contains(dt.DayOfWeek))
					yield return dt;
				dt = dt.AddDays(1);
			}
		}

		#region Extenders to create dates
		public static DateTime January(this int day, int year) {
			return new DateTime(year, 1, day);
		}
		public static DateTime February(this int day, int year) {
			return new DateTime(year, 2, day);
		}
		public static DateTime March(this int day, int year) {
			return new DateTime(year, 3, day);
		}
		public static DateTime April(this int day, int year) {
			return new DateTime(year, 4, day);
		}
		public static DateTime May(this int day, int year) {
			return new DateTime(year, 5, day);
		}
		public static DateTime June(this int day, int year) {
			return new DateTime(year, 6, day);
		}
		public static DateTime July(this int day, int year) {
			return new DateTime(year, 7, day);
		}
		public static DateTime August(this int day, int year) {
			return new DateTime(year, 8, day);
		}
		public static DateTime September(this int day, int year) {
			return new DateTime(year, 9, day);
		}
		public static DateTime October(this int day, int year) {
			return new DateTime(year, 10, day);
		}
		public static DateTime November(this int day, int year) {
			return new DateTime(year, 11, day);
		}
		public static DateTime December(this int day, int year) {
			return new DateTime(year, 12, day);
		}
		#endregion

		#region Min/Max
		public static DateTime Min(DateTime dt1, DateTime dt2) {
			if (dt1 > dt2)
				return dt2;
			else
				return dt1;
		}

		public static TimeSpan Min(TimeSpan dt1, TimeSpan dt2) {
			if (dt1 > dt2)
				return dt2;
			else
				return dt1;
		}

		public static DateTime Max(DateTime dt1, DateTime dt2) {
			if (dt1 > dt2)
				return dt1;
			else
				return dt2;
		}

		public static TimeSpan Max(TimeSpan dt1, TimeSpan dt2) {
			if (dt1 > dt2)
				return dt1;
			else
				return dt2;
		}
		#endregion

		/// <summary>
		/// Returns every possible start time, broken up to the minute, that will allow an appointment of the specified duration
		/// within the specified period.
		/// </summary>
		/// <param name="windowStart"></param>
		/// <param name="windowEnd"></param>
		/// <param name="duration"></param>
		/// <returns></returns>
		public static List<DateTime> GetAllPossibleStartTimes(DateTime windowStart, DateTime windowEnd, TimeSpan duration) {
			List<DateTime> startTimes = new List<DateTime>();
			DateTime st = windowStart;
			while (st.Add(duration) <= windowEnd) {
				startTimes.Add(st);
				st = st.AddMinutes(1);
			}
			return startTimes;
		}

		#region Overlapping periods
		public static bool PeriodsOverlap(DateTime p1Start, int p1DurationMinutes, DateTime p2Start, DateTime p2End) {
			return PeriodsOverlap(p1Start, p1Start.AddMinutes(p1DurationMinutes), p2Start, p2End);
		}
		public static bool PeriodsOverlap(DateTime p1Start, DateTime p1End, DateTime p2Start, DateTime p2End) {
			return p1Start.Between(p2Start, p2End) || p2Start.Between(p1Start, p1End);
		}
		#endregion
	}

	public static class ConsoleUtility {
		public static string GetResponse(string Message) {
			Console.WriteLine(Message);
			return Console.ReadLine();
		}
	}

	public static class TextUtility {
		/// <summary>
		/// Capitalizes the first character of each word.
		/// </summary>
		/// <param name="s"></param>
		/// <returns></returns>
		public static string CapitalizeFirstLetter(string s) {
			if (s == null || s.Length == 0)
				return s;
			s = s.ToLower();
			Regex regex = new Regex(@"\b.");
			s = regex.Replace(s, new MatchEvaluator(MatchToUpper));

			return s;
		}

		private static string MatchToUpper(Match m) {
			return m.ToString().ToUpper();
		}

		/// <summary>
		/// works pretty well.  splits a string and ignore splitcharateres inside of quoted text
		/// Returns tokens without quotes (if quoted).  Also returns empty tokens for ,,
		/// Also, it is expected that the quote appears rightafter the seperator (no extra spaces) 
		/// so a,"b,b",c will work, but a, "b,b", c will not treat the "b,b" as quotesed test and youwillget an extra token back.
		/// also if there are extra quotes, they are included in the token.  """"ABCD""","""EFG""" will return 2 tokens with 
		/// the start and end quotes not there, but the other quotes will be there
		/// </summary>
		/// <param name="s"></param>
		/// <param name="SplitChar"></param>
		/// <param name="QuotedTexted">this could be smarter by checking the length and using the second char for close quote</param>
		/// <returns></returns>
		public static string[] SmartSplit(string s, string SplitChar, string QuotedTexted) {
			if (!s.Contains(QuotedTexted))
				return s.Split(SplitChar.ToCharArray());

			List<string> parts = new List<string>();
			int len = s.Length;
			string EndChar;
			int EndCharLen = 0;
			while (s.Length > 0) {
				string c = s.Substring(0, 1);
				if (c == SplitChar) {
					parts.Add("");
					s = s.Substring(1);
					continue;
				}

				if (c == QuotedTexted) {
					EndChar = QuotedTexted + SplitChar;
					EndCharLen = 2;
					s = s.Substring(1);
				} else {
					EndChar = SplitChar;
					EndCharLen = 1;
				}

				string token;
				int index = s.IndexOf(EndChar);
				if (index < 0) {
					//if this was the last token, then the above index would not have found that closing quote - cause it is not
					//followed by a SplitChar
					if (EndCharLen == 2) {
						index = s.IndexOf(QuotedTexted);
						if (index > -1)
							s = s.Remove(index, 1);
					}
					parts.Add(s);
					s = null;
					break;
				} else {
					token = s.Substring(0, index);
				}

				parts.Add(token);
				s = s.Substring(index + EndCharLen);
			}

			if (s != null)
				parts.Add("");

			return parts.ToArray();
		}

		/// <summary>
		/// compute checksum - i am not sure if this is a real checksum or not, its based ons omething i found
		/// in ping.cs 
		/// </summary>
		/// <param name="s"></param>
		/// <returns></returns>
		public static int CheckSum(string s) {
			// DEVELOPERS - PLEASE DO NOT MESS WITH THIS CALCULATION - if you do you will break freeze and thaw

			int cksum = 0;
			foreach (char c in s.ToCharArray()) {
				cksum += (int)c;
			}

			cksum = (cksum >> 16) + (cksum & 0xffff);
			cksum += (cksum >> 16);
			return (~cksum);
		}

		/// <summary>
		/// convert a string in the format field=value,field=value into a hash.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static Dictionary<string, string> FieldValuePairs(string str) {
			string[] pairs = str.Split(" ,".ToCharArray());
			Dictionary<string, string> ht = new Dictionary<string, string>();
			foreach (string pair in pairs) {
				string[] parts = pair.Split("=".ToCharArray(), 2);
				ht[parts[0]] = parts[1];
			}

			return ht;
		}

		/// <summary>
		/// Returns whether the specified string is numeric or not
		/// </summary>
		/// <param name="inputData"></param>
		/// <returns></returns>
		public static bool IsNumeric(string inputData) {
			try {
				int.Parse(inputData);
				return true;
			} catch {
				return false;
			}
		}
		/// <summary>
		/// Splits the given string at capital letters or numerals, grouping consecutive capitals into one word
		/// </summary>
		/// <remarks>Thanks to SembleWare for this utility.</remarks>
		/// <param name="Value"></param>
		/// <returns></returns>
		public static string SpaceAtCapitals(this string Value) {
			if (Value != null) {
				System.Text.StringBuilder s = new System.Text.StringBuilder(Value.Length + 1);
				char[] c = Value.ToCharArray();
				bool bPreviousWasCapital = true;
				for (int i = 0; i < c.Length; i++) {
					if ((c[i].CompareTo('A') >= 0 && c[i].CompareTo('Z') <= 0) || (c[i].CompareTo('0') >= 0 && c[i].CompareTo('9') <= 0)) { //Upper OR Numeric
						if (!bPreviousWasCapital) {
							bPreviousWasCapital = true;
							s.Append(" ");
						}
					} else {
						bPreviousWasCapital = false;
					}
					s.Append(c[i]);
				}
				return s.ToString();
			} else {
				return null;
			}
		}
		/// <summary>
		/// Converts an digital ordinal to a string, e.g. 33rd = THIRTY THIRD
		/// </summary>
		/// <param name="NumericOrdinal"></param>
		/// <returns></returns>
		/// <remarks>Only handles numbers up to the thousands, no more.</remarks>
		public static string ConvertOrdinal(string NumericOrdinal) {
			string NumberPart = Regex.Replace(NumericOrdinal, @"\D", "");
			try {
				int Number = int.Parse(NumberPart);
				// don't bother with anything more than thousands - too rare to bother with
				int Thousands = Number / 1000;
				int Hundreds = (Number % 1000) / 100;
				int Tens = (Number % 100) / 10;
				int Units = Number % 10;
				string Result = "", Join = "";
				if (Thousands > 0) {
					Result = DigitToString(Thousands) + " THOUSAND";
					Join = " AND ";
				}
				if (Hundreds > 0) {
					Result += " " + DigitToString(Hundreds) + " HUNDRED";
					Result = Result.Trim();
					Join = " AND ";
				}
				if (Tens > 1) {
					Result += Join + DigitToTens(Tens);
					Join = " ";
				} else if (Tens == 1) {
					// special case - the teens
					Result += Join + TeenToString(10 + Units) + "TH";
					return Result;
				}
				if (Units > 0) {
					Result += Join + DigitToUnits(Units);
				} else {
					if (Result.Substring(Result.Length - 1) == "Y") { // if the number ends with tens...
						Result = Result.Substring(0, Result.Length - 1) + "IE";
					}
					Result += "TH";
				}
				return Result;
			} catch {
				// any conversion issues - just return the original value
				return NumericOrdinal;
			}
		}
		/// <summary>
		/// Converts a single non-zero digit to an ordinal unit string, e.g. 1 = FIRST.
		/// </summary>
		/// <param name="NonZeroDigit"></param>
		/// <returns></returns>
		public static string DigitToUnits(int NonZeroDigit) {
			switch (NonZeroDigit) {
				case 1:
					return "FIRST";
				case 2:
					return "SECOND";
				case 3:
					return "THIRD";
				case 4:
					return "FOURTH";
				case 5:
					return "FIFTH";
				case 6:
					return "SIXTH";
				case 7:
					return "SEVENTH";
				case 8:
					return "EIGHTH";
				case 9:
					return "NINTH";
			}
			throw new Exception("DigitToString cannot handle value " + NonZeroDigit);
		}
		/// <summary>
		/// Converts a number between 10 and 19 to a string, e.g. 11 = ELEVEN.
		/// </summary>
		/// <param name="Teen"></param>
		/// <returns></returns>
		public static string TeenToString(int Teen) {
			switch (Teen) {
				case 10:
					return "TEN";
				case 11:
					return "ELEVEN";
				case 12:
					return "TWELVE";
				case 13:
					return "THIRTEEN";
				case 14:
					return "FOURTEEN";
				case 15:
					return "FIFTEEN";
				case 16:
					return "SIXTEEN";
				case 17:
					return "SEVENTEEN";
				case 18:
					return "EIGHTEEN";
				case 19:
					return "NINETEEN";
			}
			throw new Exception("TeenToString cannot handle value " + Teen);
		}
		/// <summary>
		/// Converts a digit between 2 and 9 to a tens string, e.g. 2 = TWENTY.
		/// </summary>
		/// <param name="Tens"></param>
		/// <returns></returns>
		public static string DigitToTens(int Tens) {
			switch (Tens) {
				case 2:
					return "TWENTY";
				case 3:
					return "THIRTY";
				case 4:
					return "FORTY";
				case 5:
					return "FIFTY";
				case 6:
					return "SIXTY";
				case 7:
					return "SEVENTY";
				case 8:
					return "EIGHTY";
				case 9:
					return "NINETY";
			}
			throw new Exception("DigitToTens cannot handle value " + Tens);
		}
		/// <summary>
		/// Converts a non-zero digit to a word representation, e.g. 1 = ONE.
		/// </summary>
		/// <param name="NonZeroDigit"></param>
		/// <returns></returns>
		public static string DigitToString(int NonZeroDigit) {
			switch (NonZeroDigit) {
				case 1:
					return "ONE";
				case 2:
					return "TWO";
				case 3:
					return "THREE";
				case 4:
					return "FOUR";
				case 5:
					return "FIVE";
				case 6:
					return "SIX";
				case 7:
					return "SEVEN";
				case 8:
					return "EIGHT";
				case 9:
					return "NINE";
			}
			throw new Exception("DigitToString cannot handle value " + NonZeroDigit);
		}

		/// <summary>
		/// Returns the SoundEx ("sounds-like") value of the supplied string.
		/// </summary>
		/// <param name="s"></param>
		/// <returns></returns>
		public static string SoundEx(string s) {
			if (s.Length == 0)
				return "0000";

			char[] c = s.ToUpper().Trim().ToCharArray();
			string Result = c[0].ToString();
			char LastLetter = c[0];
			for (int n = 1; n < c.Length; n++) {
				if (c[n] == LastLetter) {
					continue;
				}
				switch (c[n]) {
					case 'B':
					case 'F':
					case 'P':
					case 'V':
						Result += "1";
						break;
					case 'C':
					case 'G':
					case 'J':
					case 'K':
					case 'Q':
					case 'S':
					case 'X':
					case 'Z':
						Result += "2";
						break;
					case 'D':
					case 'T':
						Result += "3";
						break;
					case 'L':
						Result += "4";
						break;
					case 'M':
					case 'N':
						Result += "5";
						break;
					case 'R':
						Result += "6";
						break;
					default:
						Result += "0";
						break;
				}
				LastLetter = c[n];
			}
			return Result;
		}

		public static byte[] StringToByteArray(string input) {
			System.Text.ASCIIEncoding oASCIIEncoding = new System.Text.ASCIIEncoding();
			return oASCIIEncoding.GetBytes(input);
		}

		public static string ByteArrayToString(byte[] input) {
			return System.Text.Encoding.ASCII.GetString(input);
		}

		public static string KansasDBAConvert(string instr) {
			string buf3 = "";
			string buf2 = instr;
			System.Text.RegularExpressions.Regex reg = new Regex("[-/(),+.$]");
			buf2 = reg.Replace(buf2, " ");
			reg = new Regex("['*]");
			buf2 = reg.Replace(buf2, "");

			reg = new Regex(@"\s*&\s*");
			buf2 = reg.Replace(buf2, " and ");
			string[] words = buf2.Split(new Char[] { ' ' });
			for (int i = 0; i < words.Length; ++i) {
				if (words[i].Length >= 2)
					buf3 += words[i].Substring(0, 1).ToUpper() + words[i].Substring(1).ToLower() + " ";
				else
					buf3 += words[i] + " ";
			}

			reg = new Regex("  +");
			buf3 = reg.Replace(buf3, " ");
			buf3.Trim();
			return buf3;
		}

		public static string FormatZip(string InputZip) {
			string strModifiedZip;
			if (InputZip.Length > 5) {
				strModifiedZip = String.Format("{0}-{1}", InputZip.Substring(0, 5), InputZip.Substring(5));
			} else {
				strModifiedZip = InputZip;
			}
			return strModifiedZip.Trim().Replace("  ", " ");
		}

		/// <summary>
		/// removes str from the end of string.  If str is an empty string, the last charater is removed fro Src
		/// </summary>
		/// <param name="Src"></param>
		/// <param name="str"></param>
		/// <returns>SrcStr woth str remvoed or just SrcStr id str was not removed</returns>
		public static string Chop(string SrcStr, string str) {
			if (str == null || SrcStr == null)
				return SrcStr;

			int StrLen = str.Length;
			int SrcLen = SrcStr.Length;
			if (StrLen >= SrcLen)
				return SrcStr;

			if (StrLen == 0)
				return SrcStr.Substring(0, SrcLen - 1);

			if (SrcStr.EndsWith(str))
				return SrcStr.Substring(0, SrcLen - StrLen);

			return SrcStr;
		}
	}

	public static class CommandLineOptions {
		public static int SimpleGetOpts(string[] Argv, ref Dictionary<string, string> h) {
			int i;
			string p;
			string c;

			int NumParms = Argv.Length;
			for (i = 0; i < NumParms; ) {
				c = Argv[i].Substring(0, 1);
				if (!c.Equals("-"))
					return -1 * i;

				p = Argv[i].Substring(1);
				i++;
				//check next element to see if it's a new parm or a value for the current parm
				if (i < NumParms) {
					if (Argv[i].StartsWith("-"))
						h[p] = true.ToString();
					else {
						h[p] = Argv[i];
						i++;
					}
				} else
					h[p] = true.ToString();
			}

			return 1;
		}
	}

	public static class FileUtility {
		#region Declarations
		private const string DELIMITER_BACKSLASH = @"\";
		private const string DELIMITER_FORWARDSLASH = @"/";
		#endregion
		/// <summary>
		/// move a src file to the dest file.  paths should be complete.  It will do the move even if the dest file exist
		/// </summary>
		/// <param name="src"></param>
		/// <param name="dest"></param>
		public static void Move(string src, string dest) {
			string tmpfile = null;
			if (File.Exists(dest)) {
				tmpfile = dest + DateTime.Now.Ticks.ToString();
				File.Move(dest, tmpfile);
			}

			try {
				File.Move(src, dest);
			} catch (Exception x) {
				//put back orig file
				File.Move(tmpfile, dest);
				throw x;
			}

			if (tmpfile != null)
				File.Delete(tmpfile);

		}

		public static bool IsUNCPath(string path) {
			//  FIRST, check if this is a URL or a UNC path; do this by attempting to construct uri object from it
			Uri url = new Uri(path);

			if (url.IsUnc) {
				//  it is a unc path, return true
				return true;
			} else {
				return false;
			}
		}
		/// <summary>
		/// Takes a UNC or URL path, determines which it is (NOT hardened against bad strings, assumes one or the other is present)
		/// and returns the path with correct trailing slash--UNC==back URL==forward
		/// </summary>
		/// <param name="path">URL or UNC</param>
		/// <returns>path with correct terminal slash</returns>
		public static string AppendSlashURLorUNC(string path) {
			if (IsUNCPath(path)) {
				//  it is a unc path, so decorate the end with a back-slash (to correct misconfigurations, defend against trivial errors)
				return AppendTerminalBackSlash(path);
			} else {
				//  assume URL here
				return AppendTerminalForwardSlash(path);
			}
		}
		/// <summary>
		/// Takes url-friendly paths such as "/foo/bar/file.txt" and converts to UNC-friendly paths by simple substitution, 
		/// --> "\foo\bar\file.txt"
		/// </summary>
		/// <param name="path">the url path</param>
		/// <returns>the UNC path</returns>
		public static string ConvertToUNCPath(string path) {
			return path.Replace(DELIMITER_FORWARDSLASH, DELIMITER_BACKSLASH);
		}
		/// <summary>
		/// If not present appends terminal backslash to paths
		/// </summary>
		/// <param name="path">path for example "C:\AppUpdaterClient"</param>
		/// <returns>path with trailing backslash--"C:\AppUpdaterClient\"</returns>
		public static string AppendTerminalBackSlash(string path) {
			if (path.IndexOf(DELIMITER_BACKSLASH, path.Length - 1) == -1) {
				return path + DELIMITER_BACKSLASH;
			} else {
				return path;
			}
		}
		/// <summary>
		/// Appends a terminal forward-slash if there is not already one, returns corrected path
		/// </summary>
		/// <param name="path">the path that may be missing a terminal forward-slash</param>
		/// <returns>the corrected path with terminal forward-slash</returns>
		public static string AppendTerminalForwardSlash(string path) {
			if (path.IndexOf(DELIMITER_FORWARDSLASH, path.Length - 1) == -1) {
				return path + DELIMITER_FORWARDSLASH;
			} else {
				return path;
			}
		}
		/// <summary>
		/// Given a file path such as "C:\foo\file.txt" this extracts the local root directory path, "C:\foo\" 
		/// complete with terminal backslash
		/// </summary>
		/// <param name="path">the full file path</param>
		/// <returns>the local root directory (strips terminal file name)</returns>
		public static string GetRootDirectoryFromFilePath(string path) {
			return AppendTerminalBackSlash(Path.GetDirectoryName(path));
		}
		/// <summary>
		/// Used to delete a directory and all its contents.  
		/// </summary>
		/// <param name="path">full path to directory</param>
		public static void DeleteDirectory(string path) {
			if (Directory.Exists(path)) {
				try {
					Directory.Delete(path, true);
				} catch (Exception e) {
					//  throw, this is serious enough to halt:
					throw e;
				}
			}
		}
		/// <summary>
		/// deletes and thre recreates a directory
		/// </summary>
		/// <param name="path"></param>
		public static void CleanDir(string path) {
			DeleteDirectory(path);
			Directory.CreateDirectory(path);
		}
		/// <summary>
		/// Returns the path of the executing assembly in standard Windows format ("C:\MyDir\"), complete with trailing backslash.
		/// </summary>
		/// <returns></returns>
		public static string GetCurrentAssemblyPath() {
			string path = Assembly.GetExecutingAssembly().CodeBase.Replace("/", @"\").Replace(@"file:\\\", "");
			path = Path.GetDirectoryName(path) + @"\";
			return path;
		}
		/// <summary>
		/// Used to delete a file  
		/// </summary>
		/// <param name="path">full path to file</param>
		public static void DeleteFile(string path) {
			if (File.Exists(path)) {
				try {
					File.Delete(path);
				} catch (Exception e) {
					//  throw, this is serious enough to halt:
					throw e;
				}
			}
		}
		/// <summary>
		/// Copy files from source to destination directories.  Directory.Move not suitable here because
		/// downloader may still have temp dir locked
		/// </summary>
		/// <param name="sourcePath">source path</param>
		/// <param name="destPath">destination path</param>
		public static void CopyDirectory(string sourcePath, string destPath) {
			//  put paths into DirectoryInfo object to validate 
			DirectoryInfo dirInfoSource = new DirectoryInfo(sourcePath);
			DirectoryInfo dirInfoDest = new DirectoryInfo(destPath);

			//  check if destination dir exists, if so delete it
			if (Directory.Exists(dirInfoDest.FullName)) {
				Directory.Delete(dirInfoDest.FullName, true);
			}
			//  make new dir named with new version #
			Directory.CreateDirectory(dirInfoDest.FullName);
			//  do recursive copy of temp dir to new version # dir--
			//  YES we could use one-line "Directory.Move" but _temp dir may be locked by other thread_
			CopyDirRecurse(dirInfoSource.FullName, dirInfoDest.FullName);
		}
		/// <summary>
		/// Utility function that recursively copies directories and files.
		/// Again, we could use Directory.Move but we need to preserve the original.
		/// </summary>
		/// <param name="sourcePath"></param>
		/// <param name="destinationPath"></param>
		private static void CopyDirRecurse(string sourcePath, string destinationPath) {
			//  ensure terminal backslash
			sourcePath = FileUtility.AppendTerminalBackSlash(sourcePath);
			destinationPath = FileUtility.AppendTerminalBackSlash(destinationPath);

			//  get dir info which may be file or dir info object
			DirectoryInfo dirInfo = new DirectoryInfo(sourcePath);

			foreach (FileSystemInfo fsi in dirInfo.GetFileSystemInfos()) {
				if (fsi is FileInfo) {
					//  if file object just copy
					File.Copy(fsi.FullName, destinationPath + fsi.Name);
				} else {
					//  must be a directory, create destination sub-folder and recurse to copy files
					Directory.CreateDirectory(destinationPath + fsi.Name);
					CopyDirRecurse(fsi.FullName, destinationPath + fsi.Name);
				}
			}
		}
		/// <summary>
		/// Returns the content of a file as a byte array
		/// </summary>
		/// <param name="path"></param>
		/// <returns></returns>
		public static byte[] FileToByteArray(string path) {
			// Declare File Stream
			System.IO.FileStream oF;
			byte[] BLOB;
			// Handle All Local Exception
			try {
				// Make sure we have a vaild filepath
				if (path.Length.Equals(0))
					throw new System.IO.FileNotFoundException("File: " + path + " does not exist.", path);
				// Open the File
				oF = System.IO.File.Open(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
				// Read content into a Bit Array
				BLOB = new byte[oF.Length];
				oF.Read(BLOB, 0, BLOB.Length - 1);
				// Close File
				oF.Close();
			} catch (System.Exception ex) {
				throw new Exception(ex.ToString(), ex);
			}
			// Return Document Object to client
			return BLOB;
		}
		/// <summary>
		/// Converts a byte array to a file, saved in the given path.
		/// </summary>
		/// <param name="Blob"></param>
		/// <param name="FullPath">The full path, including the file name.</param>
		/// <returns></returns>
		public static void ByteArrayToFile(byte[] Blob, string FullPath) {
			FileStream fs = new FileStream(FullPath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
			fs.Position = 0;
			fs.Write(Blob, 0, Blob.Length);
			fs.Close();
		}

		public static void Unzipper(string infile, string outdir) {
			if (infile == null || infile == "") {
				throw new Exception("No Input File Specified");
			}

			CleanDir(outdir);

			ZipInputStream ZipReader = new ZipInputStream(File.OpenRead((infile)));
			ZipEntry ze;
			try {
				while ((ze = ZipReader.GetNextEntry()) != null) {
					string OutputFileName = Path.Combine(outdir, ze.Name);
					DateTime dtStamp = ze.DateTime;
					FileStream OutFile = new FileStream(OutputFileName, FileMode.OpenOrCreate, FileAccess.Write);
					byte[] buffer = new byte[ze.Size];
					ZipReader.Read(buffer, 0, buffer.Length);
					OutFile.Write(buffer, 0, buffer.Length);
					OutFile.Close();
					FileInfo f = new FileInfo(OutputFileName);
					f.LastWriteTime = dtStamp;
				}
			} catch (Exception x) {
				// for debugging purposes
				throw new Exception(x.Message, x);
			}
		}
		/// <summary>
		/// Quietly prints the specified PDF file
		/// </summary>
		/// <param name="blob"></param>
		public static void PrintPDF(byte[] blob) {
			string TargetPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.InternetCache), Guid.NewGuid() + ".pdf");
			FileUtility.ByteArrayToFile(blob, TargetPath);
			PrintPDF(TargetPath);
		}
		/// <summary>
		/// Quietly prints the specified PDF file
		/// </summary>
		/// <param name="FullPath"></param>
		public static void PrintPDF(string FullPath) {
			// can't use Acrobat API; it only works for the registered version.  The Adobe Reader API is completely useless.
			string AcrobatPath = RegistryUtility.GetRegistryValueAsString(Registry.LocalMachine, @"SOFTWARE\Adobe\Acrobat Reader\8.0\InstallPath", "");
			Process command = new Process();
			command.StartInfo.FileName = Path.Combine(AcrobatPath, "acrord32.exe");
			command.StartInfo.Arguments = "/p /h \"" + FullPath + "\"";
			command.StartInfo.RedirectStandardOutput = true;
			command.StartInfo.UseShellExecute = false;
			command.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
			command.Start();
		}
		/// <summary>
		/// Combines any number of PDF blobs into one big ol' PDF document.
		/// </summary>
		/// <param name="NewFileName">The new file name to save.</param>
		/// <param name="PDFFileNames">The existing PDF file names.</param>
		public static void CombinePDFs(string NewFileName, params string[] PDFFileNames) {
			Debug.Fail("Untested code!  Shaul just put this here coz the interface might be useful, but it has not been tested!");
			List<byte[]> Blobs = new List<byte[]>();
			foreach (string FileName in PDFFileNames) {
				Blobs.Add(FileToByteArray(FileName));
			}
			byte[][] blobs = Blobs.ToArray();
			CombinePDFs(NewFileName, blobs);
		}
		/// <summary>
		/// Combines any number of PDF blobs into one big ol' PDF document.
		/// </summary>
		/// <param name="NewFileName">The new file name to save.</param>
		/// <param name="PDFFiles">The blobs of the component PDFs.</param>
		public static void CombinePDFs(string NewFileName, params byte[][] PDFFiles) {
			FileStream fstream = new FileStream(NewFileName, FileMode.Create);
			Document doc = new Document();
			PdfCopy pdfcp = new PdfCopy(doc, fstream);
			doc.Open();

			foreach (byte[] blob in PDFFiles) {
				PdfReader pdfr = new PdfReader(blob);
				for (int i = 1; i <= pdfr.NumberOfPages; ++i) {
					PdfImportedPage pip = pdfcp.GetImportedPage(pdfr, i);
					pdfcp.AddPage(pip);
				}
			}
			doc.Close();
			pdfcp.Close();
		}
	}

	public static class ErrorUtility {
		public static string GetCallStack() {
			// Create a StackTrace that captures
			// filename, line number and column
			// information, for the current thread.
			StackTrace st = new StackTrace(true);
			string ret = "";
			for (int i = 0; i < st.FrameCount; i++) {
				// High up the call stack, there is only one stack frame
				StackFrame sf = st.GetFrame(i);
				ret += sf.GetMethod() + ":" + sf.GetFileLineNumber().ToString() + "\n";
			}

			return ret;
		}
		/// <summary>
		/// Returns the innermost nested exception of an exception (i.e. the one that started all the trouble).
		/// </summary>
		/// <param name="x"></param>
		/// <returns></returns>
		public static Exception GetInnermostException(Exception x) {
			while (x.InnerException != null) {
				x = x.InnerException;
			}
			return x;
		}

		public static void FailsafeErrorLog(Exception x, string DataType) {
			try {
				AuditLog log = new AuditLog(0, DataType, 0, x.ToString(), 0, null);
				log.Save(AuditLog.CompletionStatus.Error, x.ToString());
			} catch {
				// drop it - the show must go on
			}
		}

		public static void FailsafeAuditLog(string msg, string DataType) {
			try {
				AuditLog log = new AuditLog(0, DataType, 0, msg, 0, null);
				log.Save(AuditLog.CompletionStatus.Error, msg);
			} catch {
				// drop it - the show must go on
			}
		}
	}

	public class DebugUtility {
		static DebugUtility() {
			FileName = "DebugLog.txt";
			DebugLevel = 0;
			__SynchLock = new object();
		}
		public static string FileName;
		public static int DebugLevel;
		private static object __SynchLock;

		private static DebugUtility __Instance;

		private static DebugUtility GetInstance() {
			if (__Instance == null)
				__Instance = new DebugUtility();
			return __Instance;
		}

		private StreamWriter mOut;
		private DebugUtility() {
			string FileName = Path.Combine(Directory.GetCurrentDirectory(), DebugUtility.FileName);
			mOut = new StreamWriter(FileName, false);
			mOut.AutoFlush = true;
		}
		/// <summary>
		/// This log only works if DebugLevel is >0.
		/// </summary>
		/// <param name="Message"></param>
		public static void Log(string Message) {
			if (DebugLevel <= 0)
				return;

			string LogMsg = string.Format("Thread {0} - {1} - {2}", Thread.CurrentThread.ManagedThreadId.ToString("00"),
				DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss.fff"), Message);
			lock (__SynchLock) {
				GetInstance().mOut.WriteLine(LogMsg);
				//Console.WriteLine(LogMsg);
			}
		}
	}

	public static class EncryptionUtil {
		static EncryptionUtil() {
			KEY_64 = new byte[] { 42, 16, 93, 156, 78, 4, 218, 32 };
			IV_64 = new byte[] { 55, 103, 246, 79, 36, 99, 167, 3 };
			KEY_192 = new byte[] { 42, 16, 93, 156, 78, 4, 218, 32, 15, 167, 
 				44, 80, 26, 250, 155, 112, 2, 94, 11, 204, 
 				119, 35, 184, 197 };
			IV_192 = new byte[] { 55, 103, 246, 79, 36, 99, 167, 3, 42, 5, 
   			62, 83, 184, 7, 209, 13, 145, 23, 200, 58, 
   			173, 10, 121, 222 };
		}
		#region Simple encryption - XOr
		internal static string SimpleEncrypt(string Value, string EncryptionKey) {
			return XOrStrings(Value, EncryptionKey);
		}

		internal static byte[] SimpleEncrypt(byte[] Value, string EncryptionKey) {
			return XOrByteArray(Value, EncryptionKey);
		}

		internal static string SimpleDecrypt(string Value, string EncryptionKey) {
			return XOrStrings(Value, EncryptionKey);
		}

		internal static byte[] SimpleDecrypt(byte[] Value, string EncryptionKey) {
			return XOrByteArray(Value, EncryptionKey);
		}

		private static byte[] XOrByteArray(byte[] Value, string EncryptionKey) {
			List<byte> Output = new List<byte>();
			for (int n = 0; n < Value.Length; n++) {
				byte b1 = Value[n];
				byte b2 = (byte)EncryptionKey[n % EncryptionKey.Length];
				byte b3 = (byte)(b1 ^ b2);
				if (b3 == 0)
					Output.Add(b1);
				else
					Output.Add(b3);
			}
			return Output.ToArray();
		}

		private static string XOrStrings(string Value, string EncryptionKey) {
			string Output = "";
			for (int n = 0; n < Value.Length; n++) {
				byte b1 = (byte)Value[n];
				byte b2 = (byte)EncryptionKey[n % EncryptionKey.Length];
				int b3 = b1 ^ b2;
				if (b3 == 0)
					Output += (char)b1;
				else
					Output += (char)b3;
			}
			return Output;
		}
		#endregion

		#region DES encryption
		//8 bytes randomly selected for both the Key and the Initialization Vector
		//the IV is used to encrypt the first block of text so that any repetitive
		//patterns are not apparent
		private static byte[] KEY_64;
		private static byte[] IV_64;
		//24 byte or 192 bit key and IV for TripleDES
		private static byte[] KEY_192;
		private static byte[] IV_192;
		//encrypts filename and preserves file extension
		public static string EncryptFileName(string value) {
			string EncryptedFileName = null;
			string Salt = "secret" + new Random().Next(100000);
			EncryptedFileName = CreateHash(value.Substring(0, value.Length - 4), Salt) + value.Substring(value.Length - 4);
			return EncryptedFileName;
		}
		private static string CreateHash(string Value, string Salt) {
			//takes a given value and salt value and outputs a hashed string
			var ValAndSalt = string.Concat(Value, Salt);
			var HashedValue = FormsAuthentication.HashPasswordForStoringInConfigFile(ValAndSalt, "sha1");
			return HashedValue;
		}
		//Standard DES encryption
		public static string Encrypt(string value) {
			if (!string.IsNullOrEmpty(value)) {
				DESCryptoServiceProvider cryptoProvider = new DESCryptoServiceProvider();
				MemoryStream ms = new MemoryStream();
				CryptoStream cs = new CryptoStream(ms, cryptoProvider.CreateEncryptor(KEY_64, IV_64), CryptoStreamMode.Write);
				StreamWriter sw = new StreamWriter(cs);
				sw.Write(value);
				sw.Flush();
				cs.FlushFinalBlock();
				ms.Flush();
				//convert back to a string
				return Convert.ToBase64String(ms.GetBuffer(), 0, (int)ms.Length);
			} else {
				return "";
			}
		}

		//Standard DES decryption
		public static string Decrypt(string value) {
			if (!string.IsNullOrEmpty(value)) {
				DESCryptoServiceProvider cryptoProvider = new DESCryptoServiceProvider();
				//convert from string to byte array
				byte[] buffer = Convert.FromBase64String(value);
				MemoryStream ms = new MemoryStream(buffer);
				CryptoStream cs = new CryptoStream(ms, cryptoProvider.CreateDecryptor(KEY_64, IV_64), CryptoStreamMode.Read);
				StreamReader sr = new StreamReader(cs);
				return sr.ReadToEnd();
			} else {
				return "";
			}
		}
		//TRIPLE DES encryption
		public static string EncryptTripleDES(string value) {
			return EncryptTripleDES(value, KEY_192, IV_192);
		}
		public static string EncryptTripleDES(string value, bool useActiveClientKey) {
			DESKeys keys = GetActiveClientDESKeys();
			return EncryptTripleDES(value, keys.EncryptionKey, keys.InitializationVector);
		}
		public static string EncryptTripleDES(string value, string encryptionKey, string initializationVector) {
			return EncryptTripleDES(value, encryptionKey.To192BitKey(), initializationVector.To192BitKey());
		}
		public static string EncryptTripleDES(string value, byte[] encryptionKey, byte[] initializationVector) {
			if (!string.IsNullOrEmpty(value)) {
				TripleDESCryptoServiceProvider cryptoProvider = new TripleDESCryptoServiceProvider();
				MemoryStream ms = new MemoryStream();
				CryptoStream cs = new CryptoStream(ms, cryptoProvider.CreateEncryptor(encryptionKey, initializationVector), CryptoStreamMode.Write);
				StreamWriter sw = new StreamWriter(cs);
				sw.Write(value);
				sw.Flush();
				cs.FlushFinalBlock();
				ms.Flush();
				//convert back to a string
				return Convert.ToBase64String(ms.GetBuffer(), 0, (int)ms.Length);
			} else {
				return "";
			}
		}

		//TRIPLE DES decryption
		public static string DecryptTripleDES(string value) {
			return DecryptTripleDES(value, KEY_192, IV_192);
		}
		public static string DecryptTripleDES(string value, bool useActiveClientKey) {
			var keys = GetActiveClientDESKeys();
			return DecryptTripleDES(value, keys.EncryptionKey, keys.InitializationVector);
		}
		public static string DecryptTripleDES(string value, string encryptionKey, string initializationVector) {
			return DecryptTripleDES(value, encryptionKey.To192BitKey(), initializationVector.To192BitKey());
		}
		public static string DecryptTripleDES(string value, byte[] encryptionKey, byte[] initializationVector) {
			if (!string.IsNullOrEmpty(value)) {
				TripleDESCryptoServiceProvider cryptoProvider = new TripleDESCryptoServiceProvider();
				//convert from string to byte array
				byte[] buffer = Convert.FromBase64String(value);
				MemoryStream ms = new MemoryStream(buffer);
				CryptoStream cs = new CryptoStream(ms, cryptoProvider.CreateDecryptor(encryptionKey, initializationVector), CryptoStreamMode.Read);
				StreamReader sr = new StreamReader(cs);
				return sr.ReadToEnd();
			} else {
				return "";
			}
		}

		private static UniStatic<Dictionary<long, DESKeys>> __DESKeyCache = new UniStatic<Dictionary<long, DESKeys>>();
		private static Dictionary<long, DESKeys> DESKeyCache {
			get {
				if (__DESKeyCache.Value == null)
					__DESKeyCache.Value = new Dictionary<long, DESKeys>();
				return __DESKeyCache.Value;
			}
		}
		private static DESKeys GetActiveClientDESKeys() {
			if (!DESKeyCache.ContainsKey(DBLocator.ActiveClientID)) {
				string fileName = "C:\\Syntac\\ClientKeys\\{0}.key".Fmt(DBLocator.ActiveClientName);
				if (!File.Exists(fileName)) {
					if (!Directory.Exists(Path.GetDirectoryName(fileName)))
						Directory.CreateDirectory(Path.GetDirectoryName(fileName));
					StreamWriter sw = new StreamWriter(fileName);
					sw.WriteLine("{0}/{1}${2}".Fmt(DBLocator.ActiveClientName, DBLocator.ActiveClientID, DBLocator.ActiveClient.CreatedDate.Ticks));
					sw.WriteLine(Guid.NewGuid().ToString());
					sw.Close();
				}
				StreamReader sr = new StreamReader(fileName);
				string key = sr.ReadLine();
				string iv = sr.ReadLine();
				DESKeyCache[DBLocator.ActiveClientID] = new DESKeys { EncryptionKey = key, InitializationVector = iv };
			}
			return DESKeyCache[DBLocator.ActiveClientID];
		}

		private static byte[] To192BitKey(this string s) {
			var repeats = Math.Ceiling(24.0 / s.Length);
			var key = "";
			for (int n = 0; n < repeats; n++)
				key += s;
			return key.Substring(0, 24).ToByteArray();
		}

		private class DESKeys {
			public string EncryptionKey;
			public string InitializationVector;
		}
		#endregion
	}

	public static class RegistryUtility {
		/// <summary>
		/// Open as read only a registry subkey and return a specific value.
		/// Parm 1 is a Root level Registry Key (ie. Registry.LocalMachine)
		/// Parm 2 a string which evaluates to a a registry subkey.
		/// Parm 3 is a string which is the name of the value to get
		/// Parm 4 is a boolean indicating whether or not to throw an exception if a null value would be returned. T-throw F-shutup
		/// </summary>
		/// <returns>Returns value as a string, null if the value is not found.</returns>
		/// <remarks>Does not work for multi or expanded sz's. Works at least for regular sz and DWORD
		/// All exceptions are thrown to the caller.
		/// If parm 4 is true, will throw an exception if the returned value would be null. The exception will indicate what caused
		/// the null value.
		/// </remarks>

		public static string GetRegistryValueAsString(RegistryKey keyName, string subKeyName, string valueName, bool throwExceptionOnNullValue) {
			RegistryKey regSubKey;
			string valueString = null, msg = "";
			object obj;

			regSubKey = keyName.OpenSubKey(subKeyName + "");

			if (regSubKey == null) {
				if (throwExceptionOnNullValue) {
					msg = "Subkey \"" + subKeyName + "\" could not be found or could not be opened.";
				}
			} else {
				//valueString = (string)regSubKey.GetValue(valueName);
				obj = regSubKey.GetValue(valueName);
				if (obj == null) {
					if (throwExceptionOnNullValue)
						msg = "Subkey \"" + subKeyName + "\" does not contain \"" + valueName + "\".";
				} else {
					valueString = obj.ToString();
				}
				regSubKey.Close();
			}

			//if the msg is filled in it means that value would have been null and we need to throw exception
			if (msg != "")
				throw new Exception(msg);

			return valueString;
		}

		///<summary>
		///For those too lazy to set the exception flag, we override the previous method and set the boolean to true
		///</summary>

		public static string GetRegistryValueAsString(RegistryKey keyName, string subKeyName, string valueName) {
			return GetRegistryValueAsString(keyName, subKeyName, valueName, true);
		}
		/// <summary>
		/// Gets a registry key, substituting the default value if the registry key is not found.
		/// </summary>
		/// <param name="keyName"></param>
		/// <param name="subKeyName"></param>
		/// <param name="valueName"></param>
		/// <param name="DefaultValue"></param>
		/// <returns></returns>
		public static string GetRegistryValueAsString(RegistryKey keyName, string subKeyName, string valueName, string DefaultValue) {
			try {
				return GetRegistryValueAsString(keyName, subKeyName, valueName);
			} catch {
				SetRegistryValue(keyName, subKeyName, valueName, DefaultValue);
				return DefaultValue;
			}
		}

		/// <summary>
		/// Sets a registry value
		/// </summary>
		/// <param name="key"></param>
		/// <param name="subKeyName"></param>
		/// <param name="valueName"></param>
		/// <param name="newValue"></param>
		public static void SetRegistryValue(RegistryKey key, string subKeyName, string valueName, object newValue) {
			RegistryKey regSubKey = key.OpenSubKey(subKeyName, true);
			if (regSubKey == null) {
				// create it!
				regSubKey = key.CreateSubKey(subKeyName);
			}
			regSubKey.SetValue(valueName, newValue);
		}
	}

	public static class EmailUtility {
		public static void SendEmail(string From, string To, string Subject, string Body) {
			var m = new MailMessage {
				From = new MailAddress(From),
				Subject = Subject,
				Body = Body,
			};
			m.To.Add(To);
			var smtp = new SmtpClient {
				Host = "localhost",
				Port = 25,
				UseDefaultCredentials = true,
			};
			smtp.Send(m);
		}
	}

	public delegate void ParameterlessEvent();
}
