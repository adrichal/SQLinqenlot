using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SQLinqenlot {
	/// <summary>
	/// Summary description for CachedQuery.
	/// </summary>
	public class CachedQuery {
		public delegate void CacheUpdatedDelegate(DataTable dt);

		private DateTime mNextRefresh;
		private int mExpire;
		private CacheUpdatedDelegate mCB;
		private string mQuery;
		private TDatabase mDB;
		private string mLastConnectionString = "";

		/// <summary>
		/// 0 - allows infintae erros - other, number of consequtive refresh errord
		/// </summary>
		public int AllowErrors;
		private int mNumErrors;

		public bool AllowNoRows = false;

		/// <summary>
		/// Will preform the given query on the and call the CB supplied. The app must call this.CheckCache before it
		/// usies teh cache to make sure its is up to date.  Default behavior is to exception if there are no rows returned.  
		/// Use AllowNoRows property to change that
		/// </summary>
		/// <param name="db"></param>
		/// <param name="query"></param>
		/// <param name="Expiration">Minues to wait inbetween refreshes value of 0 - cache never expires</param>
		/// <param name="cb">Delagate to process the Query's result.  BE careful in multiclient mode to insure you
		/// dont mix up data for two different clients in the same cache.  This may require your app to rebuild its
		/// internal data structres it uses for the cache each time the delagte is called.</param>
		public CachedQuery(TDatabase db, string query, int Expiration, CacheUpdatedDelegate cb) {
			mCB = cb;
			mExpire = Expiration;
			mQuery = query;
			mDB = db;
		}

		public void CheckCache() {
			SqlUtil sql = SqlUtil.Get(mDB);
			//deal with multi client - if we are caching and the DB we were cachine is now mapped someplace else
			//cause the client changed
			if (DateTime.Now < mNextRefresh && mLastConnectionString == sql.DBConnectionStr)
				return;

			mLastConnectionString = sql.DBConnectionStr;

			bool error = false;
			DataTable dt = null;
			try {
				dt = sql.ExecuteSingleResultSetSQLQuery(mQuery);
			} catch {
				error = true;
			}

			if (error || dt == null || (dt.Rows.Count == 0 && !AllowNoRows)) {
				if (mNextRefresh == DateTime.MinValue)
					throw new Exception("cant init cache for query " + mQuery);

				if (AllowErrors > 0) {
					mNumErrors++;
					if (mNumErrors > AllowErrors)
						throw new Exception(String.Format("could not reload cache {0} in {1} tries",
							this.mQuery, mNumErrors));
				}

				mNextRefresh = DateTime.Now.AddMinutes(mExpire);
				return;
			}

			mNumErrors = 0;
			if (mExpire == 0)
				mNextRefresh = DateTime.MaxValue;
			else
				mNextRefresh = DateTime.Now.AddMinutes(mExpire);

			mCB(dt);
			return;
		}
	}
}
