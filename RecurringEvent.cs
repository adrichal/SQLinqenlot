using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace SQLinqenlot {
	#region Public Enum
	public enum TRecurrencePattern : byte {
		None = 0,
		Daily = 1, // every d days
		Weekly = 2, // every w weeks, on x day(s) of week
		MonthlySimple = 3, // day d of every m months
		MonthlyComplex = 4, // nth x-day of every m months
		YearlySimple = 5, // day n of month m
		YearlyComplex = 6, // nth x-day of month m
		Hourly = 7,
	}

	public enum TWeekdayOrdinal : short {
		First = 1,
		Second = 2,
		Third = 3,
		Fourth = 4,
		Last = -1,
		SecondLast = -2,
		ThirdLast = -3,
		FourthLast = -4,
	}
	#endregion

	public abstract class RecurringEvent {
		#region Statics
		/// <summary>
		/// This is a factory method to return a RecurringEvent appropriate for the given pattern.
		/// </summary>
		/// <param name="pattern">The type of recurrence.</param>
		/// <param name="StartDate">The first date on which the event occurs.</param>
		/// <param name="EndDate">The last date on which a recurrence can occur, or null if no end date.</param>
		/// <returns></returns>
		public static RecurringEvent Get(TRecurrencePattern pattern, DateTime StartDate, int DurationMinutes, DateTime? RecurUntilDate) {
			return Get(pattern, StartDate, DurationMinutes, RecurUntilDate, null);
		}
		/// <summary>
		/// This is a factory method to return a RecurringEvent appropriate for the given pattern.
		/// </summary>
		/// <param name="pattern">The type of recurrence.</param>
		/// <param name="StartDate">The first date on which the event occurs.</param>
		/// <param name="EndDate">The last date on which a recurrence can occur, or null if no end date.</param>
		/// <returns></returns>
		public static RecurringEvent Get(TRecurrencePattern pattern, DateTime StartDate, int DurationMinutes, DateTime? RecurUntilDate, string RecurrenceData) {
			RecurringEvent e;
			switch (pattern) {
				case TRecurrencePattern.None:
					e = new NonRecurringEvent(); break;
				case TRecurrencePattern.Hourly:
					e = new HourlyRecurringEvent(); break;
				case TRecurrencePattern.Daily:
					e = new DailyRecurringEvent(); break;
				case TRecurrencePattern.Weekly:
					e = new WeeklyRecurringEvent(); break;
				case TRecurrencePattern.MonthlySimple:
					e = new MonthlySimpleRecurringEvent(); break;
				case TRecurrencePattern.MonthlyComplex:
					e = new MonthlyComplexRecurringEvent(); break;
				case TRecurrencePattern.YearlySimple:
					e = new YearlySimpleRecurringEvent(); break;
				case TRecurrencePattern.YearlyComplex:
					e = new YearlyComplexRecurringEvent(); break;
				default:
					throw new EnumerationValueException(pattern);
			}
			e.StartDate = StartDate;
			e.DurationMinutes = DurationMinutes;
			e.RecurUntil = RecurUntilDate;
			if (!string.IsNullOrEmpty(RecurrenceData))
				e.RecurrenceData = RecurrenceData;
			return e;
		}

		public static DayOfWeek StringToDayOfWeek(string Day) {
			switch (Day.ToUpper().Substring(0, 2)) {
				case "SU":
					return DayOfWeek.Sunday;
				case "MO":
					return DayOfWeek.Monday;
				case "TU":
					return DayOfWeek.Tuesday;
				case "WE":
					return DayOfWeek.Wednesday;
				case "TH":
					return DayOfWeek.Thursday;
				case "FR":
					return DayOfWeek.Friday;
				case "SA":
					return DayOfWeek.Saturday;
				default:
					throw new Exception("Unrecognized day of week: " + Day);
			}
		}

		private const string RFC2445_DATE_FORMAT = "yyyyMMddTHHmmssZ";
		public static DateTime RFC2445ToDate(string s) {
			string s2 = string.Format("{0}/{1}/{2} {3}:{4}:{5}",
				s.Substring(0, 4), s.Substring(4, 2), s.Substring(6, 2), s.Substring(9, 2), s.Substring(11, 2), s.Substring(13, 2));
			DateTime dt = DateTime.Parse(s2);
			return dt;
		}
		public static string DateToRFC2445(DateTime dt) {
			return string.Format("{0:" + RFC2445_DATE_FORMAT + "}", dt);
		}
		public static string WeekdayTypeToRFC2445(TWeekdayType WeekdayType) {
			switch (WeekdayType) {
				case TWeekdayType.Day:
					return "MO,TU,WE,TH,FR,SA,SU";
				case TWeekdayType.Weekday:
					return "MO,TU,WE,TH,FR";
				case TWeekdayType.WeekendDay:
					return "SA,SU";
				default:
					return WeekdayType.ToString().Substring(0, 2).ToUpper();
			}
		}
		public static TWeekdayType RFC2445ToWeekdayType(string s) {
			switch (s) {
				case "MO,TU,WE,TH,FR,SA,SU":
					return TWeekdayType.Day;
				case "MO,TU,WE,TH,FR":
					return TWeekdayType.Weekday;
				case "SA,SU":
					return TWeekdayType.WeekendDay;
				default:
					return (TWeekdayType)StringToDayOfWeek(s);
			}
		}

		protected static bool PeriodsOverlap(DateTime p1Start, int p1DurationMinutes, DateTime p2Start, DateTime p2End) {
			return DateTimeUtility.PeriodsOverlap(p1Start, p1DurationMinutes, p2Start, p2End);
		}
		protected static bool PeriodsOverlap(DateTime p1Start, DateTime p1End, DateTime p2Start, DateTime p2End) {
			return DateTimeUtility.PeriodsOverlap(p1Start, p1End, p2Start, p2End);
		}
		#endregion

		#region Instance methods
		/// <summary>
		/// Alerts a listener that the recurrence data has been changed.
		/// </summary>
		public event ParameterlessEvent RecurrenceDataChanged = delegate { };

		protected DateTime StartDate { get; set; }
		public DateTime? RecurUntil { get; set; }
		protected int DurationMinutes { get; set; }
		/// <summary>
		/// Returns or sets the recurrence data in string form.
		/// </summary>
		public abstract string RecurrenceData { get; set; }
		/// <summary>
		/// Returns all occurrences of this event between the given dates.
		/// </summary>
		/// <param name="FromDate"></param>
		/// <param name="ToDate"></param>
		/// <returns></returns>
		public abstract SortedDictionary<DateTime, DateTime> GetOccurrences(DateTime FromDate, DateTime ToDate);
		/// <summary>
		/// Returns the recurrence info in a RFC2445 style format
		/// </summary>
		/// <returns></returns>
		/// <remarks>
		/// This format is usable by the Telerik RadScheduler.  It happens to be based on the RFC 2445 standard http://tools.ietf.org/html/rfc2445 
		/// but it doesn't use it completely
		/// and therefore I felt justified in just writing a wrapper for this little piece of the spec. - SGB
		/// </remarks>
		public abstract string RFC2445RecurrenceString { get; set; }

		protected bool _SuppressRaiseEvent = false;
		protected void RaiseRecurrenceDataChanged() {
			if (!_SuppressRaiseEvent)
				RecurrenceDataChanged();
		}

		protected string RFC2445EndDate {
			get {
				if (RecurUntil != null)
					return "UNTIL=" + DateToRFC2445(RecurUntil.Value) + ";";
				else
					return "";
			}
		}

		public static Dictionary<string, string> GetRuleProperties(string RRuleLine) {
			Dictionary<string, string> RRuleProps = new Dictionary<string, string>();
			foreach (string r in RRuleLine.Split(';')) {
				string[] s = r.Split('=');
				RRuleProps[s[0]] = s[1];
			}
			return RRuleProps;
		}
		#endregion
	}

	public class NonRecurringEvent : RecurringEvent {
		public override string RecurrenceData {
			get { return ""; }
			set { /* ignore */ }
		}
		public override SortedDictionary<DateTime, DateTime> GetOccurrences(DateTime FromDate, DateTime ToDate) {
			SortedDictionary<DateTime, DateTime> result = new SortedDictionary<DateTime, DateTime>();
			// only thing to check is if the one instance of this event is in the given date range.
			if (PeriodsOverlap(StartDate, DurationMinutes, FromDate, ToDate))
				result[StartDate] = StartDate;
			return result;
		}
		public override string RFC2445RecurrenceString {
			get { return ""; }
			set {	/* ignore	*/ }
		}
	}

	public class HourlyRecurringEvent : RecurringEvent {
		#region Specific Interface
		private int _IntervalHours = 1;
		public int IntervalHours {
			get { return _IntervalHours; }
			set {
				if (value < 1)
					throw new Exception("Interval must be a positive number");
				_IntervalHours = value;
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion

		#region Overrides
		public override string RecurrenceData {
			get { return IntervalHours.ToString(); }
			set { IntervalHours = int.Parse(value); }
		}

		public override SortedDictionary<DateTime, DateTime> GetOccurrences(DateTime FromDate, DateTime ToDate) {
			SortedDictionary<DateTime, DateTime> result = new SortedDictionary<DateTime, DateTime>();
			// find the difference in days between the Start date and the From date, then iterate to find all occurrences until we pass the To date
			double HoursFromStart = FromDate.Subtract(StartDate).TotalHours;
			if (HoursFromStart < 0)
				HoursFromStart = 0;
			// since the recurrence is every h hours, we need to add a multiple of h that is >= HoursFromStart.
			int Intervals = (int)Math.Ceiling(HoursFromStart / IntervalHours);
			DateTime NextOccurrence = StartDate.AddHours(IntervalHours * Intervals);
			if (RecurUntil != null && ToDate > RecurUntil)
				ToDate = (DateTime)RecurUntil;
			while (NextOccurrence <= ToDate) {
				if (PeriodsOverlap(NextOccurrence, DurationMinutes, FromDate, ToDate))
					result[NextOccurrence] = NextOccurrence;
				NextOccurrence = NextOccurrence.AddHours(IntervalHours);
			}
			return result;
		}

		public override string RFC2445RecurrenceString {
			get {
				string s = string.Format("FREQ=HOURLY;{0}INTERVAL={1}", RFC2445EndDate, IntervalHours);
				return s;
			}
			set {
				Dictionary<string, string> props = GetRuleProperties(value);
				IntervalHours = DataUtils.ToInt(props["INTERVAL"]);
				if (props.ContainsKey("UNTIL")) {
					RecurUntil = RFC2445ToDate(props["UNTIL"]);
				} else if (props.ContainsKey("COUNT")) {
					int Count = DataUtils.ToInt(props["COUNT"]);
					// add Count-1 intervals for the end date
					RecurUntil = StartDate.AddHours((Count - 1) * IntervalHours);
				} else {
					RecurUntil = null;
				}
			}
		}
		#endregion
	}

	public class DailyRecurringEvent : RecurringEvent {
		#region Specific interface
		private int _IntervalDays = 1;
		public int IntervalDays {
			get { return _IntervalDays; }
			set {
				if (value < 1)
					throw new Exception("Interval must be a positive number");
				if (value == _IntervalDays)
					return;
				_IntervalDays = value;
				RaiseRecurrenceDataChanged();
			}
		}
		private bool _EveryWeekday = false;
		public bool EveryWeekday {
			get { return _EveryWeekday; }
			set {
				if (value == _EveryWeekday)
					return;
				_EveryWeekday = value;
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion

		#region Overrides
		public override string RecurrenceData {
			get {
				if (EveryWeekday)
					return "W";
				else
					return IntervalDays.ToString();
			}
			set {
				try {
					_SuppressRaiseEvent = true;
					if (value == "W") {
						EveryWeekday = true;
					} else {
						EveryWeekday = false;
						IntervalDays = DataUtils.ToInt(value);
					}
				} finally {
					_SuppressRaiseEvent = false;
				}
				RaiseRecurrenceDataChanged();
			}
		}

		public override SortedDictionary<DateTime, DateTime> GetOccurrences(DateTime FromDate, DateTime ToDate) {
			SortedDictionary<DateTime, DateTime> result = new SortedDictionary<DateTime, DateTime>();
			// find the difference in days between the Start date and the From date, then iterate to find all occurrences until we pass the To date
			double DaysFromStart = FromDate.Subtract(StartDate).TotalDays;
			if (DaysFromStart < 0)
				DaysFromStart = 0;
			if (RecurUntil != null && ToDate > RecurUntil)
				ToDate = RecurUntil.Value;
			if (FromDate < StartDate)
				FromDate = StartDate;
			if (EveryWeekday) {
				// find first occurrence of a weekday in range
				DateTime NextOccurrence = FromDate.Date.SetTimeOfDay(StartDate.TimeOfDay);
				while (NextOccurrence <= ToDate) {
					if (!NextOccurrence.DayMatchesWeekdayType(TWeekdayType.Weekday)
						&& PeriodsOverlap(NextOccurrence, DurationMinutes, FromDate, ToDate))
						result[NextOccurrence] = NextOccurrence;
					NextOccurrence = NextOccurrence.AddDays(1);
				}
			} else {
				// since the recurrence is every d days, we need to add a multiple of d that is >= DaysFromStart.
				int Intervals = (int)Math.Ceiling((double)DaysFromStart / IntervalDays);
				DateTime NextOccurrence = StartDate.AddDays(IntervalDays * Intervals);
				while (NextOccurrence <= ToDate) {
					if (PeriodsOverlap(NextOccurrence, DurationMinutes, FromDate, ToDate))
						result[NextOccurrence] = NextOccurrence;
					NextOccurrence = NextOccurrence.AddDays(IntervalDays);
				}
			}
			return result;
		}

		public override string RFC2445RecurrenceString {
			get {
				string s = string.Format("FREQ=DAILY;{0}INTERVAL={1};BYDAY=MO,TU,WE,TH,FR{2}", RFC2445EndDate, IntervalDays, EveryWeekday ? "" : ",SA,SU");
				return s;
			}
			set {
				Dictionary<string, string> props = GetRuleProperties(value);
				IntervalDays = DataUtils.ToInt(props["INTERVAL"]);
				if (props.ContainsKey("BYDAY") && props["BYDAY"] == "MO,TU,WE,TH,FR") {
					EveryWeekday = true;
				} else {
					EveryWeekday = false;
				}
				if (props.ContainsKey("UNTIL")) {
					RecurUntil = RFC2445ToDate(props["UNTIL"]);
				} else if (props.ContainsKey("COUNT")) {
					int Jumps = DataUtils.ToInt(props["COUNT"]) - 1; // called jumps, coz that's the number of spaces between the first (0th) instance and the last one.
					// calculated differently if by interval / every weekday
					if (EveryWeekday) {
						// add 7 days for every complete 5 days in count, then add the remaining days
						int Weeks = Jumps / 5;
						int Days = Jumps % 5;
						DateTime dt = StartDate.AddDays(7 * Weeks);
						while (Days > 0) {
							dt = dt.AddDays(1);
							if (dt.DayOfWeek != DayOfWeek.Saturday && dt.DayOfWeek != DayOfWeek.Sunday)
								Days--;
						}
						RecurUntil = dt;
					} else {
						// add Count-1 intervals for the end date
						RecurUntil = StartDate.AddDays(Jumps * IntervalDays);
					}
				} else {
					RecurUntil = null;
				}

			}
		}
		#endregion
	}

	public class WeeklyRecurringEvent : RecurringEvent {
		#region Specific interface
		private int _IntervalWeeks = 1;
		public int IntervalWeeks {
			get { return _IntervalWeeks; }
			set {
				if (value < 1)
					throw new Exception("Interval must be a positive number");
				_IntervalWeeks = value;
				RaiseRecurrenceDataChanged();
			}
		}
		private List<DayOfWeek> _DaysOfWeek = new List<DayOfWeek>();

		public void AddDayOfWeek(DayOfWeek d) {
			if (!_DaysOfWeek.Contains(d)) {
				_DaysOfWeek.Add(d);
				RaiseRecurrenceDataChanged();
			}
		}
		public void RemoveDayOfWeek(DayOfWeek d) {
			if (_DaysOfWeek.Contains(d)) {
				_DaysOfWeek.Remove(d);
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion

		#region Overrides
		public override string RecurrenceData {
			get {
				string DoW = "", Join = "";
				foreach (DayOfWeek d in _DaysOfWeek) {
					DoW = string.Format("{0}{1}{2}", DoW, Join, (int)d);
					Join = ",";
				}
				return string.Format("{0}:{1}", IntervalWeeks, DoW);
			}
			set {
				try {
					_SuppressRaiseEvent = true;
					string[] vars = value.Split(':');
					IntervalWeeks = int.Parse(vars[0]);
					string[] DoW = vars[1].Split(',');
					_DaysOfWeek.Clear();
					foreach (string s in DoW) {
						DayOfWeek d = (DayOfWeek)int.Parse(s);
						AddDayOfWeek(d);
					}
				} finally {
					_SuppressRaiseEvent = false;
				}
				RaiseRecurrenceDataChanged();
			}
		}

		public override SortedDictionary<DateTime, DateTime> GetOccurrences(DateTime FromDate, DateTime ToDate) {
			SortedDictionary<DateTime, DateTime> result = new SortedDictionary<DateTime, DateTime>();
			// just have to go through every date in the range and see if it matches...
			if (FromDate < StartDate)
				FromDate = StartDate;
			if (RecurUntil != null && RecurUntil < ToDate)
				ToDate = (DateTime)RecurUntil;
			double DaysFromStart = FromDate.Subtract(StartDate).TotalDays;
			// since the recurrence is every 7w days, we need to add a multiple of 7w that is >= DaysFromStart.
			int Intervals = (int)Math.Ceiling(DaysFromStart / (IntervalWeeks * 7));
			DateTime NextSunday = StartDate.AddDays(Intervals * IntervalWeeks * 7).AddDays(-(int)StartDate.DayOfWeek); // sunday is 0
			while (NextSunday <= ToDate) {
				DateTime NextDay = NextSunday;
				while (NextDay <= ToDate) {
					if (NextDay >= FromDate && _DaysOfWeek.Contains(NextDay.DayOfWeek)
						&& PeriodsOverlap(NextDay, DurationMinutes, FromDate, ToDate))
						result[NextDay] = NextDay;
					NextDay = NextDay.AddDays(1);
					if (NextDay.DayOfWeek == DayOfWeek.Sunday)
						break; // on to next week
				}
				NextSunday = NextSunday.AddDays(IntervalWeeks * 7);
			}
			return result;
		}

		public override string RFC2445RecurrenceString {
			get {
				string Days = string.Join(",", _DaysOfWeek.Select(dw => dw.ToString().Substring(0, 2).ToUpper()).ToArray());
				string s = string.Format("FREQ=WEEKLY;{0}INTERVAL={1};BYDAY={2}", RFC2445EndDate, IntervalWeeks, Days);
				return s;
			}
			set {
				var props = GetRuleProperties(value);
				string[] Days = props["BYDAY"].Split(',');
				_DaysOfWeek.Clear();
				Days.ToList().ForEach(d => _DaysOfWeek.Add(StringToDayOfWeek(d)));
				IntervalWeeks = DataUtils.ToInt(props["INTERVAL"]);
				if (props.ContainsKey("COUNT")) {
					// tricky, this... first find the number of complete weeks to skip, based on how many days in the week we're using
					int Jumps = DataUtils.ToInt(props["COUNT"]) - 1;
					int CompleteIntervals = Jumps / _DaysOfWeek.Count;
					int Remainder = Jumps % _DaysOfWeek.Count;
					DateTime dt = StartDate.AddDays(7 * IntervalWeeks * CompleteIntervals);
					while (Remainder > 0) {
						dt = dt.AddDays(1);
						if (_DaysOfWeek.Contains(dt.DayOfWeek))
							Remainder--;
					}
					RecurUntil = dt;
				} else if (props.ContainsKey("UNTIL")) {
					RecurUntil = RFC2445ToDate(props["UNTIL"]);
				} else {
					RecurUntil = null;
				}
			}
		}
		#endregion
	}

	public class MonthlySimpleRecurringEvent : RecurringEvent {
		#region Specific interface
		private int _DayOfMonth = 1;
		public int DayOfMonth {
			get { return _DayOfMonth; }
			set {
				if (value < 1 || value > 31)
					throw new Exception("Day of month must be a number between 1 and 31");
				_DayOfMonth = value;
				RaiseRecurrenceDataChanged();
			}
		}
		private int _IntervalMonths = 1;
		public int IntervalMonths {
			get { return _IntervalMonths; }
			set {
				if (value < 1)
					throw new Exception("Interval must be a positive number");
				_IntervalMonths = value;
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion

		#region Overrides
		public override string RecurrenceData {
			get {
				return string.Format("{0}:{1}", DayOfMonth, IntervalMonths);
			}
			set {
				try {
					_SuppressRaiseEvent = true;
					string[] vars = value.Split(':');
					DayOfMonth = int.Parse(vars[0]);
					IntervalMonths = int.Parse(vars[1]);
				} finally {
					_SuppressRaiseEvent = false;
				}
				RaiseRecurrenceDataChanged();
			}
		}

		public override SortedDictionary<DateTime, DateTime> GetOccurrences(DateTime FromDate, DateTime ToDate) {
			SortedDictionary<DateTime, DateTime> result = new SortedDictionary<DateTime, DateTime>();
			if (FromDate < StartDate)
				FromDate = StartDate;
			if (RecurUntil != null && RecurUntil < ToDate)
				ToDate = (DateTime)RecurUntil;
			int FromMonth = FromDate.Month, FromYear = FromDate.Year;
			// could be that StartDate is not actually an occurrence date; we use the specified DayOfMonth in the month of StartDate as the first occurrence
			DateTime FirstOccurrence = DateTimeUtility.SafeGetDate(StartDate.Year, StartDate.Month, DayOfMonth, StartDate.Hour, StartDate.Minute, StartDate.Second, StartDate.Millisecond);
			int MonthsFromStart = (FromYear - FirstOccurrence.Year) * 12 + FromMonth - FirstOccurrence.Month;
			// since the recurrence is every m months, we need to add a multiple of m that is >= MonthsFromStart.
			int Intervals = (int)Math.Ceiling((double)MonthsFromStart / IntervalMonths);
			int AddMonths = Intervals * IntervalMonths;
			DateTime NextOccurrence = FirstOccurrence.AddMonths(AddMonths);
			while (NextOccurrence <= ToDate) {
				if (PeriodsOverlap(NextOccurrence, DurationMinutes, FromDate, ToDate))
					result[NextOccurrence] = NextOccurrence;
				AddMonths += IntervalMonths; // always work off the same baseline start date so that shorter months don't throw the calculation out.
				NextOccurrence = FirstOccurrence.AddMonths(AddMonths);
			}
			return result;
		}

		public override string RFC2445RecurrenceString {
			get {
				string s = string.Format("FREQ=MONTHLY;{0}INTERVAL={1};BYMONTHDAY={2}", RFC2445EndDate, IntervalMonths, DayOfMonth);
				return s;
			}
			set {
				var props = GetRuleProperties(value);
				try {
					_SuppressRaiseEvent = true;
					IntervalMonths = DataUtils.ToInt(props["INTERVAL"]);
					DayOfMonth = DataUtils.ToInt(props["BYMONTHDAY"]);
					if (props.ContainsKey("UNTIL")) {
						RecurUntil = RFC2445ToDate(props["UNTIL"]);
					} else if (props.ContainsKey("COUNT")) {
						int Jumps = DataUtils.ToInt(props["COUNT"]) - 1;
						RecurUntil = StartDate.AddMonths(Jumps * IntervalMonths);
					} else {
						RecurUntil = null;
					}
				} finally {
					_SuppressRaiseEvent = false;
				}
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion
	}

	public class MonthlyComplexRecurringEvent : RecurringEvent {
		#region Specific interface
		private TWeekdayOrdinal _Ordinal = TWeekdayOrdinal.First;
		public TWeekdayOrdinal Ordinal {
			get { return _Ordinal; }
			set {
				_Ordinal = value;
				RaiseRecurrenceDataChanged();
			}
		}
		private TWeekdayType _WeekdayType = TWeekdayType.Day;
		public TWeekdayType WeekdayType {
			get { return _WeekdayType; }
			set {
				_WeekdayType = value;
				RaiseRecurrenceDataChanged();
			}
		}
		private int _IntervalMonths = 1;
		public int IntervalMonths {
			get { return _IntervalMonths; }
			set {
				if (value < 1)
					throw new Exception("Interval must be a positive number");
				_IntervalMonths = value;
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion

		#region Overrides
		public override string RecurrenceData {
			get {
				return string.Format("{0}:{1}:{2}", (int)Ordinal, (int)WeekdayType, IntervalMonths);
			}
			set {
				try {
					_SuppressRaiseEvent = true;
					string[] vars = value.Split(':');
					Ordinal = DataUtils.ToEnum<TWeekdayOrdinal>(vars[0]);
					WeekdayType = DataUtils.ToEnum<TWeekdayType>(vars[1]);
					IntervalMonths = int.Parse(vars[2]);
				} finally {
					_SuppressRaiseEvent = false;
				}
				RaiseRecurrenceDataChanged();
			}
		}

		public override SortedDictionary<DateTime, DateTime> GetOccurrences(DateTime FromDate, DateTime ToDate) {
			SortedDictionary<DateTime, DateTime> result = new SortedDictionary<DateTime, DateTime>();
			if (FromDate < StartDate)
				FromDate = StartDate;
			if (RecurUntil != null && RecurUntil < ToDate)
				ToDate = (DateTime)RecurUntil;
			int FromMonth = FromDate.Month, FromYear = FromDate.Year;
			// could be that StartDate is not actually an occurrence date; we use the specified week/day in the month of StartDate as the first occurrence
			int MonthsFromStart = (FromYear - StartDate.Year) * 12 + FromMonth - StartDate.Month;
			// since the recurrence is every m months, we need to add a multiple of m that is >= MonthsFromStart.
			int Intervals = (int)Math.Ceiling((double)MonthsFromStart / IntervalMonths);
			int AddMonths = Intervals * IntervalMonths;
			DateTime NextOccurrenceMonth = StartDate.AddMonths(AddMonths);
			DateTime NextOccurrence = DateTimeUtility.GetDate(NextOccurrenceMonth.Year, NextOccurrenceMonth.Month, this.WeekdayType, (int)Ordinal)
				.AddHours(StartDate.Hour).AddMinutes(StartDate.Minute).AddSeconds(StartDate.Second);
			while (NextOccurrence <= ToDate) {
				if (PeriodsOverlap(NextOccurrence, DurationMinutes, FromDate, ToDate))
					result[NextOccurrence] = NextOccurrence;
				AddMonths += IntervalMonths; // always work off the same baseline start date so that shorter months don't throw the calculation out.
				NextOccurrenceMonth = StartDate.AddMonths(AddMonths);
				NextOccurrence = DateTimeUtility.GetDate(NextOccurrenceMonth.Year, NextOccurrenceMonth.Month, this.WeekdayType, (int)Ordinal)
					.AddHours(StartDate.Hour).AddMinutes(StartDate.Minute).AddSeconds(StartDate.Second);
			}
			return result;
		}

		public override string RFC2445RecurrenceString {
			get {
				string s = string.Format("FREQ=MONTHLY;{0}INTERVAL={1};BYSETPOS={2};BYDAY={3}",
					RFC2445EndDate, IntervalMonths, (int)_Ordinal, WeekdayTypeToRFC2445(WeekdayType));
				return s;
			}
			set {
				try {
					var props = GetRuleProperties(value);
					_SuppressRaiseEvent = true;
					IntervalMonths = DataUtils.ToInt(props["INTERVAL"]);
					Ordinal = DataUtils.ToEnum<TWeekdayOrdinal>(props["BYSETPOS"]);
					WeekdayType = RFC2445ToWeekdayType(props["BYDAY"]);
					if (props.ContainsKey("UNTIL")) {
						RecurUntil = RFC2445ToDate(props["UNTIL"]);
					} else if (props.ContainsKey("COUNT")) {
						int Jumps = DataUtils.ToInt(props["COUNT"]) - 1;
						RecurUntil = StartDate.AddMonths(Jumps * IntervalMonths);
					} else {
						RecurUntil = null;
					}
				} finally {
					_SuppressRaiseEvent = false;
				}
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion
	}

	public class YearlySimpleRecurringEvent : RecurringEvent {
		#region Specific interface
		private DateTime _RecurrenceDate;
		public DateTime RecurrenceDate {
			get { return _RecurrenceDate; }
			set {
				if (_RecurrenceDate == value)
					return;
				_RecurrenceDate = value;
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion

		#region Overrides
		public override string RecurrenceData {
			get { return string.Format("{0:MMdd}", RecurrenceDate); }
			set {
				if (value.Length != 4)
					throw new Exception("Recurrence data must be a four digit string representing MMdd.");
				int Month = DataUtils.ToInt(value.Substring(0, 2));
				int Day = DataUtils.ToInt(value.Substring(2, 2));
				// year is irrelevant, but choose a leap year in case someone recurs every Feb 29.
				RecurrenceDate = new DateTime(2008, Month, Day);
			}
		}

		public override SortedDictionary<DateTime, DateTime> GetOccurrences(DateTime FromDate, DateTime ToDate) {
			SortedDictionary<DateTime, DateTime> result = new SortedDictionary<DateTime, DateTime>();
			if (FromDate < StartDate)
				FromDate = StartDate;
			if (RecurUntil != null && RecurUntil < ToDate)
				ToDate = (DateTime)RecurUntil;
			// start date might not be the same as initially defined!
			StartDate = DateTimeUtility.SafeGetDate(StartDate.Year, RecurrenceDate.Month, RecurrenceDate.Day);
			int YearsFromStart = FromDate.Year - StartDate.Year;
			DateTime NextOccurrence = StartDate.AddYears(YearsFromStart);
			while (NextOccurrence <= ToDate) {
				if (PeriodsOverlap(NextOccurrence, DurationMinutes, FromDate, ToDate))
					result[NextOccurrence] = NextOccurrence;
				// always work off the same baseline start date so that shorter months don't throw the calculation out.
				NextOccurrence = StartDate.AddYears(++YearsFromStart);
			}
			return result;
		}

		public override string RFC2445RecurrenceString {
			get {
				string s = string.Format("FREQ=YEARLY;{0}BYMONTHDAY={1};BYDAY=MO,TU,WE,TH,FR,SA,SU;BYMONTH={2}",
					RFC2445EndDate, RecurrenceDate.Day, RecurrenceDate.Month);
				return s;
			}
			set {
				var props = GetRuleProperties(value);
				int Month = DataUtils.ToInt(props["BYMONTH"]);
				int Day = DataUtils.ToInt(props["BYMONTHDAY"]);
				// start date might not be the same as initially defined!
				StartDate = new DateTime(StartDate.Year, Month, Day);
				RecurrenceDate = StartDate;
				if (props.ContainsKey("UNTIL")) {
					RecurUntil = RFC2445ToDate(props["UNTIL"]);
				} else if (props.ContainsKey("COUNT")) {
					int Jumps = DataUtils.ToInt(props["COUNT"]) - 1;
					RecurUntil = StartDate.AddYears(Jumps);
				} else {
					RecurUntil = null;
				}
			}
		}
		#endregion
	}

	public class YearlyComplexRecurringEvent : RecurringEvent {
		#region Specific interface
		private TWeekdayOrdinal _Ordinal = TWeekdayOrdinal.First;
		public TWeekdayOrdinal Ordinal {
			get { return _Ordinal; }
			set {
				_Ordinal = value;
				RaiseRecurrenceDataChanged();
			}
		}
		private TWeekdayType _WeekdayType = TWeekdayType.Day;
		public TWeekdayType WeekdayType {
			get { return _WeekdayType; }
			set {
				_WeekdayType = value;
				RaiseRecurrenceDataChanged();
			}
		}
		private int _Month = 1;
		public int Month {
			get { return _Month; }
			set {
				if (value < 1 || value > 12)
					throw new Exception("Month must be between 1 and 12.");
				_Month = value;
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion

		#region Overrides
		public override string RecurrenceData {
			get {
				return string.Format("{0}:{1}:{2}", (int)Ordinal, (int)WeekdayType, Month);
			}
			set {
				try {
					_SuppressRaiseEvent = true;
					string[] vars = value.Split(':');
					Ordinal = DataUtils.ToEnum<TWeekdayOrdinal>(vars[0]);
					WeekdayType = DataUtils.ToEnum<TWeekdayType>(vars[1]);
					Month = DataUtils.ToInt(vars[2]);
				} finally {
					_SuppressRaiseEvent = false;
				}
				RaiseRecurrenceDataChanged();
			}
		}

		public override SortedDictionary<DateTime, DateTime> GetOccurrences(DateTime FromDate, DateTime ToDate) {
			SortedDictionary<DateTime, DateTime> result = new SortedDictionary<DateTime, DateTime>();
			if (FromDate < StartDate)
				FromDate = StartDate;
			if (RecurUntil != null && RecurUntil < ToDate)
				ToDate = (DateTime)RecurUntil;
			for (int Year = FromDate.Year; Year <= ToDate.Year; Year++) {
				DateTime NextOccurrence = DateTimeUtility.GetDate(Year, Month, WeekdayType, (int)Ordinal)
					.AddHours(StartDate.Hour).AddMinutes(StartDate.Minute).AddSeconds(StartDate.Second);
				if (PeriodsOverlap(NextOccurrence, DurationMinutes, FromDate, ToDate))
					result[NextOccurrence] = NextOccurrence;
			}
			return result;
		}

		public override string RFC2445RecurrenceString {
			get {
				string s = string.Format("FREQ=YEARLY;{0}INTERVAL={1};BYSETPOS={2};BYDAY={3};BYMONTH={4}",
					RFC2445EndDate, 1, (int)Ordinal, WeekdayTypeToRFC2445(WeekdayType), Month);
				return s;
			}
			set {
				try {
					_SuppressRaiseEvent = true;
					var props = GetRuleProperties(value);
					Ordinal = DataUtils.ToEnum<TWeekdayOrdinal>(props["BYSETPOS"]);
					WeekdayType = RFC2445ToWeekdayType(props["BYDAY"]);
					Month = DataUtils.ToInt(props["BYMONTH"]);
					if (props.ContainsKey("UNTIL")) {
						RecurUntil = RFC2445ToDate(props["UNTIL"]);
					} else if (props.ContainsKey("COUNT")) {
						int Jumps = DataUtils.ToInt(props["COUNT"]) - 1;
						RecurUntil = StartDate.AddYears(Jumps).AddDays(30);
						// add a month just in case an occurrence is later in the last year.  Can't hurt, bc the repetition is every year.
					} else {
						RecurUntil = null;
					}
				} finally {
					_SuppressRaiseEvent = false;
				}
				RaiseRecurrenceDataChanged();
			}
		}
		#endregion
	}
}
