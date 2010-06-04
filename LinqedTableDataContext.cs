using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Reflection;

namespace SQLinqenlot {
	public abstract class LinqedTableDataContext : DataContext {
		#region Constructors
		public LinqedTableDataContext(string connection, MappingSource mappingSource) : base(connection, mappingSource) { }

		public LinqedTableDataContext(IDbConnection connection, MappingSource mappingSource) : base(connection, mappingSource) { }
		#endregion

		#region Data persistence
		private bool _Busy = false;
		public override void SubmitChanges(ConflictMode failureMode) {
			if (_Busy)
				return; // no action & no error; just let this SubmitChanges handle all nested submissions.
			try {
				_Busy = true;
				BeginTransaction();
				// Before doing anything, notify objects of the impending changes...
				Dictionary<LinqedTable, bool> ltUpdates = new Dictionary<LinqedTable, bool>();
				Dictionary<LinqedTable, bool> ltInserts = new Dictionary<LinqedTable, bool>();
				Dictionary<LinqedTable, bool> ltDeletes = new Dictionary<LinqedTable, bool>();

				var changeSet = GetChangeSet();
				SynchronizeChanges(ltUpdates, changeSet.Updates);
				SynchronizeChanges(ltInserts, changeSet.Inserts);
				SynchronizeChanges(ltDeletes, changeSet.Deletes);

				while (ltInserts.Any(i => i.Value == false) || ltUpdates.Any(u => u.Value == false) || ltDeletes.Any(d => d.Value == false)) {
					List<LinqedTable> tmp = ltInserts.Where(i => i.Value == false).Select(i => i.Key).ToList();
					foreach (LinqedTable lt in tmp) {
						lt.BeforeInsert();
						ltInserts[lt] = true;
						// auto-audit happens after the save, so that we can get the generated ID value
					}
					tmp = ltUpdates.Where(u => u.Value == false).Select(u => u.Key).ToList();
					foreach (LinqedTable lt in tmp) {
						lt.BeforeUpdate();
						ltUpdates[lt] = true;
						if (lt.AutoAudit) {
							LinqedTable orig = (LinqedTable)GetTable(lt.GetType()).GetOriginalEntityState(lt);
							AuditHeaderBase ah = GetNewAuditHeader();
							ah.MyDataID = lt.IDValue;
							ah.MyEventDescription = "Update";
							ah.MyDataType = lt.GetType().Name;
							foreach (PropertyInfo prop in LinqedTable.GetProperties(lt.GetType()).Values) {
								object OldValue = prop.GetValue(orig, null);
								object NewValue = prop.GetValue(lt, null);
								if ((OldValue == null ^ NewValue == null) || (OldValue != null && NewValue != null && !OldValue.Equals(NewValue))) {
									AuditDetailBase ad = GetNewAuditDetail(ah);
									ad.MyField = prop.Name;
									ad.MyOldValue = DataUtils.BlankIfNull(OldValue).ToString();
									ad.MyNewValue = DataUtils.BlankIfNull(NewValue).ToString();
								}
							}
						}
					}
					tmp = ltDeletes.Where(d => d.Value == false).Select(d => d.Key).ToList();
					foreach (LinqedTable lt in tmp) {
						lt.BeforeDelete();
						ltDeletes[lt] = true;
						if (lt.AutoAudit) {
							LinqedTable orig = (LinqedTable)GetTable(lt.GetType()).GetOriginalEntityState(lt);
							AuditHeaderBase ah = GetNewAuditHeader();
							ah.MyDataID = lt.IDValue;
							ah.MyEventDescription = "Delete";
							ah.MyDataType = lt.GetType().Name;
							foreach (PropertyInfo prop in LinqedTable.GetProperties(lt.GetType()).Values) {
								AuditDetailBase ad = GetNewAuditDetail(ah);
								ad.MyField = prop.Name;
								ad.MyOldValue = DataUtils.BlankIfNull(prop.GetValue(orig, null)).ToString();
							}
						}
					}
					// before allowing us to proceed with the SubmitChanges, make sure that any LinqedTables with triggered changes also get BeforeUpdate() called.
					changeSet = GetChangeSet();
					SynchronizeChanges(ltUpdates, changeSet.Updates);
					SynchronizeChanges(ltInserts, changeSet.Inserts);
					SynchronizeChanges(ltDeletes, changeSet.Deletes);
				}
				// now submit the changes...
				try {
					base.SubmitChanges(ConflictMode.ContinueOnConflict);
				} catch (ChangeConflictException) {
					// Automerge database values for members that client
					// has not modified.
					foreach (ObjectChangeConflict occ in ChangeConflicts) {
						var mc = occ.MemberConflicts.ToList();

						occ.Resolve(RefreshMode.KeepChanges);
					}
				}
				// Submit succeeds on second try.
				base.SubmitChanges(ConflictMode.FailOnFirstConflict);
				// ... and notify the child objects that they've been acted upon
				foreach (LinqedTable lt in ltInserts.Keys) {
					lt.AfterInsert();
					lt.RaiseSavedEvent();
					if (lt.AutoAudit) {
						AuditHeaderBase ah = GetNewAuditHeader();
						ah.MyDataID = lt.IDValue;
						ah.MyEventDescription = "Insert";
						ah.MyDataType = lt.GetType().Name;
						foreach (PropertyInfo prop in LinqedTable.GetProperties(lt.GetType()).Values) {
							AuditDetailBase ad = GetNewAuditDetail(ah);
							ad.MyField = prop.Name;
							ad.MyNewValue = DataUtils.BlankIfNull(prop.GetValue(lt, null)).ToString();
						}
					}
				}
				foreach (LinqedTable lt in ltUpdates.Keys) {
					lt.AfterUpdate();
					lt.RaiseSavedEvent();
				}
				foreach (LinqedTable lt in ltDeletes.Keys) {
					lt.AfterDelete();
					lt.RaiseDeletedEvent();
				}
				CommitTransaction();
			} catch {
				RollbackTransaction();
				throw;
			} finally {
				_Busy = false;
			}
			// now, just in case any of the After... functions triggered a change:
			var cs = GetChangeSet();
			if (cs.Deletes.Count + cs.Inserts.Count + cs.Updates.Count > 0)
				SubmitChanges();
		}

		private void SynchronizeChanges(Dictionary<LinqedTable, bool> ltDict, IList<object> iList) {
			var q = iList.OfType<LinqedTable>().Where(i => !ltDict.ContainsKey(i));
			q.ToList().ForEach(i => ltDict[i] = false);
		}

		private int mTransactionNestingLevel = 0;
		/// <summary>
		/// Begins a transaction, or if already in a transaction, nests the transaction one level deeper.
		/// </summary>
		public void BeginTransaction() {
			if (mTransactionNestingLevel == 0) {
				OpenConnectionIfClosed();
				Transaction = Connection.BeginTransaction();
			}
			mTransactionNestingLevel++;
		}
		/// <summary>
		/// Commits the current transaction, or if the transaction is nested, moves out of the current nesting level.
		/// </summary>
		public void CommitTransaction() {
			if (mTransactionNestingLevel <= 0) {
				mTransactionNestingLevel = 0;
				throw new Exception("'Commit transaction' without corresponding 'Begin Transaction'.");
			}
			mTransactionNestingLevel--;
			if (mTransactionNestingLevel == 0) {
				Transaction.Commit();
				Transaction = null;
			}
		}
		/// <summary>
		/// Rolls back the current transaction and resets the nesting level.
		/// </summary>
		public void RollbackTransaction() {
			// don't throw exception if no begin transaction; could be that a rollback happened at a deeper nesting level.
			if (mTransactionNestingLevel > 0) {
				Transaction.Rollback();
				Transaction = null;
			}
			mTransactionNestingLevel = 0;
		}

		private void OpenConnectionIfClosed() {
			switch (Connection.State) {
				case ConnectionState.Broken:
				case ConnectionState.Closed:
					Connection.Open();
					break;
			}
		}
		#endregion

		#region Auto-Audit
		protected virtual string AuditHeaderClassName {
			get { return string.Format("{0}.AuditHeader", GetType().Namespace); }
		}
		protected virtual string AuditDetailClassName {
			get { return string.Format("{0}.AuditDetail", GetType().Namespace); }
		}
		protected virtual AuditHeaderBase GetNewAuditHeader() {
			AuditHeaderBase header = (AuditHeaderBase)Activator.CreateInstance(GetType().Assembly.GetType(AuditHeaderClassName));
			header.InsertOnSubmit();
			return header;
		}
		protected virtual AuditDetailBase GetNewAuditDetail(AuditHeaderBase header) {
			AuditDetailBase detail = (AuditDetailBase)Activator.CreateInstance(GetType().Assembly.GetType(AuditDetailClassName));
			detail.InsertOnSubmit();
			detail.MyAuditHeader = header;
			return detail;
		}
		#endregion
	}
}
