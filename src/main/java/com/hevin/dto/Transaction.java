package com.hevin.dto;

import java.util.HashSet;
import java.util.Set;

import com.hevin.state.IsolationLevel;
import com.hevin.state.TransactionState;

public class Transaction {

	private final IsolationLevel isolationLevel;
	private final int id;
	private TransactionState state;
	private final Set<Integer> inProgress;	// for handle RepeatableRead, Snapshot, Serializable isolation level
	private final Set<String> writeSet = new HashSet<>();
	private final Set<String> readSet = new HashSet<>();

	public static final int INVALID_TRANSACTION_ID = 0;

	public IsolationLevel getIsolationLevel() {
		return isolationLevel;
	}

	public Transaction(IsolationLevel isolationLevel, int id, TransactionState state,
			Set<Integer> inProgress) {
		this.isolationLevel = isolationLevel;
		this.id = id;
		this.state = state;
		this.inProgress = inProgress;
	}

	public int getId() {
		return id;
	}

	public TransactionState getState() {
		return state;
	}

	public void setState(TransactionState state) {
		this.state = state;
	}

	public Set<Integer> getInProgress() {
		return inProgress;
	}

	public Set<String> getWriteSet() {
		return writeSet;
	}

	public Set<String> getReadSet() {
		return readSet;
	}

}
