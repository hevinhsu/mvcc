package com.hevin;

import com.hevin.dto.Command;
import com.hevin.dto.Transaction;
import com.hevin.state.TransactionState;
import com.hevin.utils.Utils;

public class Connection {

	public static final String NO_RECORD_BE_MODIFIED = "update 0";
	public static final String[] EMPTY_ARGS = {};
	private final Database database;
	private Transaction transaction;

	public Connection(Database database) {
		this.database = database;
	}

	public void begin() {
		this.executeCommand(Command.Begin, EMPTY_ARGS);
	}

	public String commit() {
		return this.executeCommand(Command.Commit, EMPTY_ARGS);
	}

	public void abort() {
		this.executeCommand(Command.Abort, EMPTY_ARGS);
	}

	public String set(String key, String value) {
		return this.executeCommand(Command.Set, new String[]{key, value});
	}

	public String get(String key) {
		return this.executeCommand(Command.Get, new String[]{key});
	}

	public String delete(String key) {
		return this.executeCommand(Command.Delete, new String[]{key});
	}

	private String executeCommand(Command command, String[] args) {
		Utils.debug("executing command: " + command + ", args: " + String.join(",", args));
		switch (command) {
			case Begin -> {
				validate(command, args);

				transaction = database.newTransaction();
				database.assertValidateTransaction(transaction);
				return "";
			}
			case Abort -> {
				validate(command, args);

				database.completeTransaction(transaction, TransactionState.Aborted);
				this.transaction = null;
				return "";
			}
			case Commit -> {
				validate(command, args);

				try {
					database.completeTransaction(transaction, TransactionState.Committed);
				} catch (RuntimeException e) {	// write-write conflict occurs
					this.transaction = null;
					return e.getMessage();
				}
				this.transaction = null;
				return "";
			}
			case Get -> {
				validate(command, args);

				String key = args[0];
				Utils.debug("get key: " + key);

				transaction.getReadSet().add(key);

				return database.getVisibleValue(transaction, key)
						.orElse(NO_RECORD_BE_MODIFIED);
			}
			
			case Set, Delete -> {
				validate(command, args);
				String key = args[0];
				Utils.debug("get key: " + key);

				if (command == Command.Delete && !database.setEndTxIdToVisibleValues(transaction, key)) {
					return NO_RECORD_BE_MODIFIED;
				}

				transaction.getWriteSet().add(key);

				if(command == Command.Set) {
					String value = args[1];
					Utils.debug("get value: " + value);
					database.upsert(transaction, key, value);
					return "";
				}

				return "";
			}
		}

		throw new RuntimeException("unsupported command: " + command);
	}


	private void validate(Command command, String[] args) {
		if (command == Command.Begin) {
			Utils.assertWith(transaction == null, "expect no running transaction.");
			return;
		}

		database.assertValidateTransaction(transaction);
		switch (command) {
			case Get -> Utils.assertWith(args.length == 1, "expect 1 argument for [get] command");
			case Set -> Utils.assertWith(args.length == 2, "expect 2 argument for [set] command");
			case Delete -> Utils.assertWith(args.length == 1, "expect 1 argument for [delete] command");
		}
	}
}
