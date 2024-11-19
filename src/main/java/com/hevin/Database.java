package com.hevin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.hevin.dto.Transaction;
import com.hevin.dto.Value;
import com.hevin.state.IsolationLevel;
import com.hevin.state.TransactionState;
import com.hevin.utils.Utils;

public class Database {

	private IsolationLevel defaultIsolationLevel;
	// in real world, this would be a b+ tree or SSTable
	// value persist value list to support snapshot isolation with different version of value
	private Map<String, List<Value>> store;
	private Map<Integer, Transaction> transactions;
	private int nextTransactionId;

	public static Database newDatabase() {
		Database database = new Database();
		database.defaultIsolationLevel = IsolationLevel.ReadCommitted;
		database.store = new HashMap<>();
		database.transactions = new HashMap<>();
		database.nextTransactionId = 0;
		return database;
	}

	public void setDefaultIsolationLevel(IsolationLevel defaultIsolationLevel) {
		this.defaultIsolationLevel = defaultIsolationLevel;
	}

	public Set<Integer> inprogress() {
		return transactions.entrySet().stream()
				.filter(e -> e.getValue().getState() == TransactionState.InProgress)
				.map(Entry::getKey)
				.collect(Collectors.toSet());
	}

	public Transaction newTransaction() {
		Transaction transaction = new Transaction(defaultIsolationLevel, ++nextTransactionId,
				TransactionState.InProgress, inprogress());
		transactions.put(transaction.getId(), transaction);
		Utils.debug("new transaction: " + transaction.getId());
		return transaction;
	}


	public void assertValidateTransaction(Transaction transaction) {
		Utils.assertWith(transaction != null, "transaction not begin.");
		Utils.assertWith(transaction.getId() > Transaction.INVALID_TRANSACTION_ID,
				"invalid transaction id");
		Utils.assertWith(transactions.get(transaction.getId()) != null, "transaction is not in exist");
		Utils.assertWith(
				transactions.get(transaction.getId()).getState() == TransactionState.InProgress,
				"transaction is not in progress");
	}


	// there are two command can complete transaction: commit and abort(rollback)
	public void completeTransaction(Transaction transaction, TransactionState state) {
		Utils.debug("completing transaction: " + transaction.getId());

		if (state == TransactionState.Committed) {
			if (transaction.getIsolationLevel() == IsolationLevel.Snapshot && hasOverlapTx(transaction,
					// modify same value by different tx
					(currentTx, overlapTx) -> checkSharedValueConflict(currentTx.getWriteSet(),
							overlapTx.getWriteSet()))) {
				completeTransaction(transaction, TransactionState.Aborted);
				throw new RuntimeException("write-write conflict");
			}

			// to make tInterleaved execution like Serializable, to prevent tx can not read/write value by other tx
			if (transaction.getIsolationLevel() == IsolationLevel.Serializable && (
					hasOverlapTx(transaction,
							(currentTx, overlapTx) -> checkSharedValueConflict(currentTx.getReadSet(),
									overlapTx.getWriteSet()))
							|| hasOverlapTx(transaction,
							(currentTx, overlapTx) -> checkSharedValueConflict(currentTx.getWriteSet(),
									overlapTx.getReadSet()))
			)
			) {
				completeTransaction(transaction, TransactionState.Aborted);
				throw new RuntimeException("read-write conflict");
			}
		}

		transaction.setState(state);
		transactions.put(transaction.getId(), transaction);
	}

	public Connection newConnection() {
		return new Connection(this);
	}


	public void upsert(Transaction tx, String key, String value) {
		List<Value> values = store.getOrDefault(key, new ArrayList<>());
		values.add(new Value(tx.getId(), value));
		store.put(key, values);
	}


	// visibility
	private boolean isVisible(Transaction tx, Value value) {
		IsolationLevel isolationLevel = tx.getIsolationLevel();
		// Read UnCommitted read the lasted value
		// event if tx that wrote values as not committed
		//, and even if ot has aborted.
		// An special case of Read UnCommitted case is delete.
		// Tx can not read delete operation even if the record is deleted without committed by other tx.
		if (isolationLevel == IsolationLevel.ReadUnCommitted) {
			return value.getTxEndId() == 0;  // make sure the value has not been deleted.
		}

		// All Committed value is visible for Read Committed at the point in time where we read.
		if (isolationLevel == IsolationLevel.ReadCommitted) {
			// exclude operations by other un committed tx
			if (value.getTxStartId() != tx.getId()  // value is not created by current tx.
					&& transactions.get(value.getTxStartId()).getState()
					!= TransactionState.Committed) {  // value is modify by other uncommitted tx
				return false;
			}

			// can not read the deleted value
			// exclude soft delete value using value.getTxEndId
			if (value.getTxEndId() == tx.getId()) { // value deleted by myself
				return false;
			}
			if (value.getTxEndId() > 0 // record be deleted
					&& transactions.get(value.getTxEndId()).getState()
					== TransactionState.Committed) {  // value is deleted by other committed tx
				return false;
			}

			return true;
		}

		// handle RepeatableRead, Snapshot, Serializable

		// can not read deleted value by myself.
		if (value.getTxEndId() == tx.getId()) {
			return false;
		}

		// ignore value created after this tx begin
		if (value.getTxStartId() > tx.getId()) {
			return false;
		}

		// ignore value is in progress before this tx begin
		// It means upsert operation is not committed before this tx begin.
		if (tx.getInProgress().contains(value.getTxStartId())) {
			return false;
		}

		// ignore uncommitted value from other tx
		if (transactions.get(value.getTxStartId()).getState() != TransactionState.Committed
				&& value.getTxStartId() != tx.getId()) {
			return false;
		}

		// focus on deleted operation before this tx start
		if (value.getTxEndId() > 0  // value is be deleted
				&& value.getTxEndId() < tx.getId()  // delete operation is before than this tx begin
				// this delete operation is committed.
				&& transactions.get(value.getTxEndId()).getState() == TransactionState.Committed
				// delete operation is committed(previous condition) before than this tx begin.
				&& !tx.getInProgress().contains(value.getTxEndId())
		) {
			return false;
		}

		return true;
	}

	public Optional<String> getVisibleValue(Transaction tx, String key) {
		List<Value> v = store.get(key);
		if (v == null) {
			return Optional.empty();
		}
		// find the value from the newest value to the oldest value.
		for (int i = v.size() - 1; i >= 0; i--) {
			Value value = v.get(i);
			if (isVisible(tx, value)) {
				return Optional.of(value.getValue());
			}
		}

		return Optional.empty();
	}

	public boolean setEndTxIdToVisibleValues(Transaction tx, String key) {
		List<Value> v = store.get(key);
		if (v == null) {
			return false;
		}

		boolean success = false;
		for (Value value : v) {
			if (isVisible(tx, value)) {
				success = true;
				value.setTxEndId(tx.getId());
			}
		}
		return success;
	}

	// conflict test for Snapshot isolation & Serializable
	// just check overlap txs
	// in original article, this method called hasConflict
	public boolean hasOverlapTx(Transaction tx,
			BiFunction<Transaction, Transaction, Boolean> conflictFn) {

		// check overlap occur in inprogress(tx before current tx and still inprogress) txs
		for (Integer inprogressId : tx.getInProgress()) {
			Transaction inprogressTx = transactions.get(inprogressId);

			// no overlap tx
			if (inprogressTx == null) {
				continue;
			}

			if (inprogressTx.getState() == TransactionState.Committed) {
				if (conflictFn.apply(tx, inprogressTx)) {
					return true;
				}
			}
		}

		// check overlap for tx created after current tx
		for (int txId = tx.getId(); txId < nextTransactionId; txId++) {
			Transaction afterTx = transactions.get(txId);
			if (afterTx == null) {
				continue;
			}

			if (afterTx.getState() == TransactionState.Committed) {
				if (conflictFn.apply(tx, afterTx)) {
					return true;
				}
			}
		}

		return false;
	}


	// Snapshot isolation check conflict function: write-write conflict
	// in original article, this method called setsShareItem
	private Boolean checkSharedValueConflict(Set<String> currentValues, Set<String> overflapValues) {
		for (String key : currentValues) {
			if (overflapValues.contains(key)) {
				return true;
			}
		}
		return false;
	}


}
