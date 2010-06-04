using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;
using System.Threading;
using System.Runtime.Serialization.Formatters.Binary;

namespace SQLinqenlot {
	/// <summary>
	/// This is a home for all the freakin' COOLEST extensions in the world!
	/// </summary>
	/// <remarks>
	/// Lifted from:
	/// http://stackoverflow.com/questions/271398/post-your-extension-goodies-for-c-net-codeplexcomextensionoverflow?answer=358259#358259
	/// </remarks>
	public static class Extensions {
		/// <summary>
		/// Enable quick and more natural string.Format calls
		/// </summary>
		/// <param name="s"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static string Fmt(this string s, params object[] args) {
			return string.Format(s, args);
		}
		/// <summary>
		/// Converts the first letter of each word in the string to upper case and the rest to lower case.
		/// </summary>
		/// <param name="s"></param>
		/// <returns></returns>
		public static string ToProper(this string s) {
			return Thread.CurrentThread.CurrentCulture.TextInfo.ToTitleCase(s);
		}
		/// <summary>
		/// Returns the rightmost n characters from the given string.
		/// </summary>
		/// <param name="str"></param>
		/// <param name="chars"></param>
		/// <returns></returns>
		public static string Right(this string str, int chars) {
			return str.Substring(str.Length - chars, chars);
		}
		/// <summary>
		/// Joins the strings in the IEnumerable with the given separator.
		/// </summary>
		/// <param name="strings"></param>
		/// <param name="separator"></param>
		/// <returns></returns>
		public static string Join(this IEnumerable<string> strings, string separator) {
			return string.Join(separator, strings.ToArray());
		}
		/// <summary>
		/// Converts the given string into a byte array
		/// </summary>
		/// <param name="s"></param>
		/// <returns></returns>
		public static byte[] ToByteArray(this string s) {
			return s.ToCharArray().Select(c => (byte)c).ToArray();
		}
		/// <summary>
		/// Returns true if this value is between the lower and upper values (inclusive).
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="actual"></param>
		/// <param name="lower"></param>
		/// <param name="upper"></param>
		/// <returns></returns>
		public static bool Between<T>(this T actual, T lower, T upper) where T : IComparable<T> {
			return actual.CompareTo(lower) >= 0 && actual.CompareTo(upper) <= 0;
		}
		/// <summary>
		/// Returns true if this value is contained in the parameter list.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="source"></param>
		/// <param name="list"></param>
		/// <returns></returns>
		public static bool In<T>(this T source, params T[] list) {
			if (null == source) throw new ArgumentNullException("source");
			return list.Contains(source);
		}
		/// <summary>
		/// Returns the given date with the time of day set as specified, seconds = 0.
		/// </summary>
		/// <param name="dt"></param>
		/// <param name="hour"></param>
		/// <param name="minute"></param>
		/// <returns></returns>
		public static DateTime SetTimeOfDay(this DateTime dt, int hour, int minute) {
			return SetTimeOfDay(dt, hour, minute, 0);
		}
		/// <summary>
		/// Returns the given date with the time of day set as specified.
		/// </summary>
		/// <param name="dt"></param>
		/// <param name="hour"></param>
		/// <param name="minute"></param>
		/// <param name="second"></param>
		/// <returns></returns>
		public static DateTime SetTimeOfDay(this DateTime dt, int hour, int minute, int second) {
			return new DateTime(dt.Year, dt.Month, dt.Day, hour, minute, second);
		}
		/// <summary>
		/// Returns the given date with the time of day set as specified.
		/// </summary>
		/// <param name="dt"></param>
		/// <param name="timeOfDay"></param>
		/// <returns></returns>
		public static DateTime SetTimeOfDay(this DateTime dt, TimeSpan timeOfDay) {
			return new DateTime(dt.Year, dt.Month, dt.Day).Add(timeOfDay);
		}
	}
}
