package com.hevin;

import com.hevin.state.IsolationLevel;
import com.hevin.utils.Utils;

public class Main {


	private static final Database database = Database.newDatabase();

	public static void main(String[] args) {
//		readUncommittedTest();
		readCommittedTest();
//		repeatableReadTest();
//		snapshotTestForWriteWriteConflict();
//		serializableReadWriteTest();
	}


	// read uncommitted can read uncommitted operation.
	// Event under the read uncommitted isolation level, it's still noe allowed
	// to read uncommitted delete operation.
	// this is a special case in read uncommitted.
	private static void readUncommittedTest() {
		database.setDefaultIsolationLevel(IsolationLevel.ReadUnCommitted);

		Connection c1 = database.newConnection();
		c1.begin();
		Connection c2 = database.newConnection();
		c2.begin();

		c1.set("x", "hey");

		// Update is visible to self.
		String retC1 = c1.get("x");
		Utils.assertWith(retC1.equals("hey"), "c1 should get x");

		// But since read uncommitted, also available to everyone else.
		String retC2 = c2.get("x");
		Utils.assertWith(retC2.equals("hey"), "c1 should get x");

		// And if we delete, that should be respected.
		retC1 = c1.delete("x");
		Utils.assertWith(retC1.isEmpty(), "c1 should delete x");

		retC1 = c1.get("x");
		Utils.assertWith(retC1.equals(Connection.NO_RECORD_BE_MODIFIED),
				"c1 should get nothing from x");

		retC2 = c2.get("x");
		Utils.assertWith(retC2.equals(Connection.NO_RECORD_BE_MODIFIED),
				"c2 should get nothing from x");

		System.out.println(
				"Read Uncommitted read isolation test pass: other tx can not read the uncommitted delete operation. But other uncommitted operations are visible.");
	}


	private static void readCommittedTest() {
		database.setDefaultIsolationLevel(IsolationLevel.ReadCommitted);

		Connection c1 = database.newConnection();
		c1.begin();
		Connection c2 = database.newConnection();
		c2.begin();

		c1.set("x", "hey");

		// Update is visible to itself.
		String retC1 = c1.get("x");
		Utils.assertWith(retC1.equals("hey"), "c1 should get x");

		// C2 can not read C1 operations
		String retC2 = c2.get("x");
		Utils.assertWith(retC2.equals(Connection.NO_RECORD_BE_MODIFIED),
				"c2 should not read c1 operations: set x");

		// C1 commit
		c1.commit();
		// C2 can read value after C1 committed
		retC2 = c2.get("x");
		Utils.assertWith(retC2.equals("hey"), "c2 can read c1 operations: set x after c1 committed");

		//

		Connection c3 = database.newConnection();
		c3.begin();

		c3.set("x", "yall");
		// can read locally
		String retC3 = c3.get("x");
		Utils.assertWith(retC3.equals("yall"), "c3 should read operations locally: set x");

		retC2 = c2.get("x");
		// can not read un committed operation again. so that value still operated by C1
		Utils.assertWith(retC2.equals("hey"),
				"c2 should not read c3 operations: set x");

		// can not read aborted operations
		c3.abort();
		retC2 = c2.get("x");
		// can not read un committed operation again
		Utils.assertWith(retC2.equals("hey"),
				"c2 should not aborted operation from C3. so the result is C1 operated");

		// delete locally
		c2.delete("x");
		retC2 = c2.get("x");
		Utils.assertWith(retC2.equals(Connection.NO_RECORD_BE_MODIFIED),
				"c2 should not read deleted value: x locally");
		c2.commit();

		// C4 can read deleted value
		Connection c4 = database.newConnection();
		c4.begin();
		String retC4 = c4.get("x");
		Utils.assertWith(retC4.equals(Connection.NO_RECORD_BE_MODIFIED),
				"c4 can read deleted value: x delete by c2");

		System.out.println("Read Committed test pass: tx can not read uncommitted values.");
	}


	private static void repeatableReadTest() {
		database.setDefaultIsolationLevel(IsolationLevel.RepeatableRead);

		Connection c1 = database.newConnection();
		c1.begin();
		Connection c2 = database.newConnection();
		c2.begin();

		c1.set("x", "hey");

		// Update is visible to myself.
		String retC1 = c1.get("x");
		Utils.assertWith(retC1.equals("hey"), "c1 should get x");

		// C1 ops is not visible to another tx.
		String retC2 = c2.get("x");
		Utils.assertWith(retC2.equals(Connection.NO_RECORD_BE_MODIFIED), "x is not visible for C2");

		c1.commit();
		retC2 = c2.get("x");
		Utils.assertWith(retC2.equals(Connection.NO_RECORD_BE_MODIFIED), "x still not visible for C2 after C1 committed in RepeatableRead");

		// but is visible for new tx
		Connection c3 = database.newConnection();
		c3.begin();
		String retC3 = c3.get("x");
		Utils.assertWith(retC3.equals("hey"), "x is visible for new tx afer C1 commit");

		// local change is visible
		c3.set("x", "yall");
		retC3 = c3.get("x");
		Utils.assertWith(retC3.equals("yall"), "local change is visible");

		// but not no the other commit, again
		retC2 = c2.get("x");
		Utils.assertWith(!retC2.equals("yall"), "c3 operation is not visible for other tx");

		c3.abort();


		// abort operation is not visible for tx start before this operation.
		retC2 = c2.get("x");
		Utils.assertWith(retC2.equals(Connection.NO_RECORD_BE_MODIFIED), "x still not existed.");


		// And again still the aborted set is still not on a new
		// transaction.
		Connection c4 = database.newConnection();
		c4.begin();
		String retC4 = c4.get("x");
		Utils.assertWith(retC4.equals("hey"), "get the C1 operation");
		c4.delete("x");
		c4.commit();

		// delete op is visible for the new tx
		Connection c5 = database.newConnection();
		c5.begin();
		String retC5 = c5.get("x");
		Utils.assertWith(retC5.equals(Connection.NO_RECORD_BE_MODIFIED), "c4 delete operation is visible for the new tx created after c4 committed.");

		System.out.println("Repeatable Read test pass");
	}

	private static void snapshotTestForWriteWriteConflict() {
		database.setDefaultIsolationLevel(IsolationLevel.Snapshot);

		Connection c1 = database.newConnection();
		c1.begin();
		Connection c2 = database.newConnection();
		c2.begin();
		Connection c3 = database.newConnection();
		c3.begin();

		c1.set("x", "hey");
		c2.set("x", "hey");

		c1.commit();
		String commit = c2.commit();
		Utils.assertWith(commit.equals("write-write conflict"), "write-write conflict should be detected");

		// no conflict
		c3.set("y", "hey");
		String noConflict = c3.commit();
		Utils.assertWith(noConflict.isEmpty(), "no conflict detected");

		System.out.println("SnapShot write-write conflict test pass");
	}

	private static void serializableReadWriteTest() {
		database.setDefaultIsolationLevel(IsolationLevel.Serializable);

		Connection c1 = database.newConnection();
		c1.begin();
		Connection c2 = database.newConnection();
		c2.begin();
		Connection c3 = database.newConnection();
		c3.begin();

		c1.set("x", "hey");
		c1.commit();
		String c1GetX = c2.get("x");
		Utils.assertWith(c1GetX.equals(Connection.NO_RECORD_BE_MODIFIED), "c2 can not read other tx operation which is not committed before c2 begin");
		String c2Commit = c2.commit();
		Utils.assertWith(c2Commit.equals("read-write conflict"), "c2 occur read-write conflict because c1 is committed after c2 begin");


		// But unrelated keys cause no conflict.
		c3.set("y", "hey");
		String c3Commit = c3.commit();
		Utils.assertWith(c3Commit.isEmpty(), "no conflict detected");

		System.out.println("Serializable read-write conflict test pass");
	}
	

}