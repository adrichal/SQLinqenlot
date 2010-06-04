using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLinqenlot {
	public enum TResultSetTypes {
		None,
		Scalar,
		Single,
		Multiple,
	}

	public enum TDatabase {
		Unknown,
		Client,
		Syntac,
		V1,
	}

	public enum TClientStatus : byte {
		Inactive = 0,
		Active = 1,
	}

	public enum TTimeUnit : byte {
		Once = 0,
		Day = 1,
		Week = 2,
		Month = 3,
		Year = 4,
		TwoWeeks = 5,
		TwoMonths = 6,
		Quarter = 7,
		HalfYear = 8,
	}

	public enum THealthProgram : byte {
		EarlyIntervention = 1,
		CPSE = 2,
		CSE = 3,
	}

	public enum TGender {
		Male,
		Female,
		Unknown
	}

	public enum TWeekdayType : int {
		Sunday = DayOfWeek.Sunday,
		Monday = DayOfWeek.Monday,
		Tuesday = DayOfWeek.Tuesday,
		Wednesday = DayOfWeek.Wednesday,
		Thursday = DayOfWeek.Thursday,
		Friday = DayOfWeek.Friday,
		Saturday = DayOfWeek.Saturday,
		Day = 8,
		Weekday = 9,
		WeekendDay = 10,
	}

	public enum TYesNoNA : byte {
		Yes = 1,
		No = 0,
		NA = 2,
	}
}
