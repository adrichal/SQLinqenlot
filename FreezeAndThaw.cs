using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Reflection;

namespace SQLinqenlot {
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	public class FreezeAndThaw {

		public static void Tester() {
			string str = null;

			Dictionary<string, object> h = new Dictionary<string, object>();
			h["{"] = "{";
			h["}"] = "}";
			h["1"] = 1;
			h["str"] = str;

			Dictionary<string, object> h2 = new Dictionary<string, object>();
			h2["a"] = System.DateTime.Now;

			h["hash"] = h2;

			int[] IntArray = new int[3];
			IntArray[2] = 2;
			h["Array"] = IntArray;

			List<object> al = new List<object>();
			al.Add("test0");
			al.Add("test1");
			h["ArrayList"] = al;

			FreezeAndThaw sh = new FreezeAndThaw();

			string s = sh.Freeze(h);
			Console.WriteLine(s);

			h2 = (Dictionary<string, object>)sh.Thaw(s);
			Console.WriteLine(h2.Count.ToString());
		}

		private const string SEP = ":";
		private const string TOKEN_SEP = ",";

		private string mStr;

		private int mPos;
		private int mEndPos;
		private int mVersion;

		#region static methods
		public static string FreezeIt(object o) {
			FreezeAndThaw f = new FreezeAndThaw();
			return f.Freeze(o);
		}

		public static object ThawIt(string s) {
			FreezeAndThaw t = new FreezeAndThaw();
			return t.Thaw(s);
		}

		#endregion

		public string Freeze(object o) {
			mStr = "";
			AddToken(o, null);
			return "2" + SEP + TextUtility.CheckSum(mStr).ToString() + SEP + mStr;
		}

		public object Thaw(string str) {
			mPos = 0;
			mStr = str;

			mVersion = GetInt();
			int CheckSumIn = GetInt();

			if (mVersion == 1 || CheckSumIn == 0) {
				//skip checksum
			} else if (mVersion == 2) {
				int CheckSum = TextUtility.CheckSum(mStr.Substring(mPos));
				if (CheckSum != CheckSumIn)
					throw new Exception("checksum miss match!");
			} else
				throw new Exception("invalid version " + mVersion.ToString());


			mEndPos = str.Length;

			return GetToken();
		}

		private Hashtable makeHash() {
			Hashtable h = new Hashtable();
			bool EndHash;
			while (mPos < mEndPos) {
				object o = GetToken(out EndHash);
				if (EndHash)
					break;

				h[o] = GetToken();
			}

			return h;
		}

		private Dictionary<TKey, TValue> makeDictionary<TKey, TValue>() {
			Dictionary<TKey, TValue> d = new Dictionary<TKey, TValue>();
			bool EndDict;
			while (mPos < mEndPos) {
				object o = GetToken(out EndDict);
				if (EndDict)
					break;
				d[(TKey)o] = (TValue)GetToken();
			}
			return d;
		}

		private Array makeArray() {
			string type = GetDataType();
			int size = GetInt();

			Array a = Array.CreateInstance(DecodeType(type), size);

			int i;
			for (i = 0; i < size; i++)
				a.SetValue(GetToken(), i);

			return a;
		}

		private static Type DecodeType(string type) {
			switch (type) {
				case "s":
					return typeof(string);
				case "t":
					return typeof(DateTime);
				case "sh":
					return typeof(short);
				case "b":
					return typeof(byte);
				case "i":
					return typeof(int);
				case "l":
					return typeof(long);
				case "B":
					return typeof(bool);
				case "c":
					return typeof(char);
				case "D":
					return typeof(decimal);
				case "d":
					return typeof(double);
				default:
					throw new Exception("can't decode type, not supported: " + type);
			}
		}

		private ArrayList makeArrayList() {
			int size = GetInt();

			ArrayList al = new ArrayList(size);
			int i;
			for (i = 0; i < size; i++)
				al.Add(GetToken());

			return al;
		}

		private List<T> makeList<T>() {
			int size = GetInt();
			List<T> result = new List<T>();
			for (int i = 0; i < size; i++)
				result.Add((T)GetToken());
			return result;
		}

		private IFreezeAndThawable makeFreezeAndThawable() {
			string dataType = GetDataType();
			IFreezeAndThawable ft = (IFreezeAndThawable)Activator.CreateInstance(Type.GetType(dataType));
			ft.Thaw(GetToken());
			return ft;
		}

		private void stringifyHash(Hashtable h) {
			IDictionaryEnumerator e;
			try {
				System.Collections.SortedList s = new SortedList(h);
				e = s.GetEnumerator();
			} catch {
				e = h.GetEnumerator();
			}

			mStr += "{";
			while (e.MoveNext()) {
				AddToken(e.Key, "=");
				AddToken(e.Value, TOKEN_SEP);
			}
			mStr += "}";
		}

		private void stringifyDictionary<TKey, TValue>(Dictionary<TKey, TValue> dict) {
			mStr += string.Format("<{0}{1}{2}{1}", EncodeDataType(typeof(TKey)), SEP, EncodeDataType(typeof(TValue)));
			foreach (KeyValuePair<TKey, TValue> kvp in dict) {
				AddToken(kvp.Key, "=");
				AddToken(kvp.Value, TOKEN_SEP);
			}
			mStr += ">";
		}

		private void stringifyArray(Array a) {
			mStr += "[" + EncodeDataType(a.GetType()) + SEP + a.Length.ToString() + SEP;
			foreach (object o in a) {
				AddToken(o, TOKEN_SEP);
			}
		}

		private void stringifyArrayList(ArrayList a) {
			mStr += "(" + a.Count.ToString() + SEP;
			foreach (object o in a) {
				AddToken(o, TOKEN_SEP);
			}
		}

		private void stringifyList<T>(List<T> list) {
			// this bears optimization - we don't need to add type information for each element if the collection type is already defined...
			mStr += string.Format("^{0}{1}{2}{1}", EncodeDataType(typeof(T)), SEP, list.Count);
			foreach (object o in list) {
				AddToken(o, TOKEN_SEP);
			}
		}

		private void stringifyFreezeAndThawable(IFreezeAndThawable ft) {
			mStr += "%" + ft.GetType().FullName + SEP;
			AddToken(ft.Freeze(), TOKEN_SEP);
		}

		private void AddToken(object o, string EndingSep) {
			if (o == null) {
				mStr += "N" + SEP + 0 + SEP;
				if (EndingSep != null)
					mStr += EndingSep;

				return;
			}

			System.Type t = o.GetType();

			if (t.IsArray)
				stringifyArray((Array)o);
			else if (o is Hashtable)
				stringifyHash((Hashtable)o);
			else if (o is ArrayList)
				stringifyArrayList((ArrayList)o);
			else if (t.IsGenericType) {
				Type GenTypeDef = t.GetGenericTypeDefinition();
				if (GenTypeDef == typeof(List<>)) {
					Type ListType = GetListType(t);
					// can you believe - we have to use reflection to call the stringify method with generics!
					typeof(FreezeAndThaw).GetMethod("stringifyList", BindingFlags.NonPublic | BindingFlags.Instance).MakeGenericMethod(ListType).Invoke(this, new object[] { o });
				} else if (GenTypeDef == typeof(Dictionary<,>)) {
					Type[] DictionaryType = GetDictionaryType(t);
					typeof(FreezeAndThaw).GetMethod("stringifyDictionary", BindingFlags.NonPublic | BindingFlags.Instance).MakeGenericMethod(DictionaryType).Invoke(this, new object[] { o });
				} else {
					throw new Exception("Generic type not supported: " + GenTypeDef.ToString());
				}
			} else if (o is IFreezeAndThawable) {
				stringifyFreezeAndThawable((IFreezeAndThawable)o);
			} else {
				string s;
				if (t.IsEnum) {
					t = Enum.GetUnderlyingType(t);
					s = Convert.ChangeType(o, t).ToString();
				} else if (t == typeof(DateTime)) {
					s = ((DateTime)o).ToString("yyyy/MM/dd HH:mm:ss.fff zzz");
				} else {
					s = o.ToString();
				}

				mStr += EncodeDataType(t) + SEP + s.Length + SEP + s;

				if (EndingSep != null)
					mStr += EndingSep;
			}
		}

		private Type[] GetDictionaryType(Type t) {
			foreach (Type intType in t.GetInterfaces()) {
				if (intType.IsGenericType
						&& intType.GetGenericTypeDefinition() == typeof(IDictionary<,>)) {
					return intType.GetGenericArguments();
				}
			}
			return null;
		}

		private static Type GetListType(Type t) {
			foreach (Type intType in t.GetInterfaces()) {
				if (intType.IsGenericType
						&& intType.GetGenericTypeDefinition() == typeof(IList<>)) {
					return intType.GetGenericArguments()[0];
				}
			}
			return null;
		}

		private object GetToken() {
			bool dummy;
			return GetToken(out dummy);
		}

		private object GetToken(out bool EndCollection) {
			EndCollection = false;

			switch (mStr.Substring(mPos, 1)) {
				case "}":
				case ">":
					mPos++;
					EndCollection = true;
					return null;

				case "{":
					mPos++;
					return makeHash();

				case "<":
					mPos++;
					string sKey = GetDataType();
					string sValue = GetDataType();
					Type[] DictType = new Type[] { DecodeType(sKey), DecodeType(sValue) };
					MethodInfo MakeDictionaryMethod = typeof(FreezeAndThaw).GetMethod("makeDictionary", BindingFlags.Instance | BindingFlags.NonPublic).MakeGenericMethod(DictType);
					return MakeDictionaryMethod.Invoke(this, null);

				case "[":
					mPos++;
					return makeArray();

				case "(":
					mPos++;
					return makeArrayList();

				case "^":
					mPos++;
					string TypeString = GetDataType();
					Type MemberType = DecodeType(TypeString);
					MethodInfo MakeListMethod = typeof(FreezeAndThaw).GetMethod("makeList", BindingFlags.NonPublic | BindingFlags.Instance).MakeGenericMethod(MemberType);
					return MakeListMethod.Invoke(this, null);

				case "%":
					mPos++;
					return makeFreezeAndThawable();
			}

			string type = GetDataType();
			int len = GetInt();

			//get data
			string data = mStr.Substring(mPos, len);
			mPos += (len + 1);

			return DecodeData(type, data);
		}

		private string EncodeDataType(System.Type t) {
			if (t == typeof(string) || t == typeof(string[]))
				return "s";
			else if (t == typeof(DateTime) || t == typeof(DateTime[]))
				return "t";
			else if (t == typeof(short) || t == typeof(short[]))
				return "sh";
			else if (t == typeof(byte) || t == typeof(byte[]))
				return "b";
			else if (t == typeof(int) || t == typeof(int[]))
				return "i";
			else if (t == typeof(long) || t == typeof(long[]))
				return "l";
			else if (t == typeof(bool) || t == typeof(bool[]))
				return "B";
			else if (t == typeof(char) || t == typeof(char[]))
				return "c";
			else if (t == typeof(decimal) || t == typeof(decimal[]))
				return "D";
			else if (t == typeof(double) || t == typeof(double[]))
				return "d";
			else
				throw new Exception("Can't encode type " + t.ToString());
		}

		private object DecodeData(string type, string data) {
			//if you add types here - be sure to add them to makeArray

			switch (type) {
				case ("N"):
					return null;
				case ("s"):
					return data;
				case ("t"):
					if (data.StartsWith("9999")) // Max Time
						return DateTime.MaxValue;
					if (Regex.IsMatch(data, @"0*1/0*1/0*1")) // Min Time
						return DateTime.MinValue;
					return Convert.ToDateTime(data);
				case ("sh"):
					return Convert.ToInt16(data);
				case ("b"):
					return Convert.ToByte(data);
				case ("B"):
					return Convert.ToBoolean(data);
				case ("i"):
					return Convert.ToInt32(data);
				case ("l"):
					return Convert.ToInt64(data);
				case ("c"):
					return Convert.ToChar(data);
				case ("D"):
					return Convert.ToDecimal(data);
				case ("d"):
					return Convert.ToDouble(data);
				default:
					throw new Exception("Can't decode, type " + type);
			}
		}

		private static Type GetEnumType(string type) {
			string EnumName = type.Substring(1);
			return Type.GetType(EnumName);
		}

		private string GetDataType() {
			int p = mStr.IndexOf(SEP, mPos);
			if (p < 1)
				throw new Exception("cant get data type " + mStr.Substring(mPos));

			string type = mStr.Substring(mPos, p - mPos);
			mPos = p + 1;

			return type;

		}

		private int GetInt() {
			int p = mStr.IndexOf(SEP, mPos);
			if (p < 1)
				throw new Exception("cant get int " + mStr.Substring(mPos));

			int len = Convert.ToInt32(mStr.Substring(mPos, p - mPos));
			mPos = p + 1;

			return len;
		}
	}

	public interface IFreezeAndThawable {
		/// <summary>
		/// Freezes this object to an object that is natively serializable by FreezeAndThaw.
		/// </summary>
		/// <returns></returns>
		object Freeze();
		/// <summary>
		/// Reconstructs the frozen data into this object.
		/// </summary>
		/// <param name="FrozenData"></param>
		void Thaw(object FrozenData);
	}
}
