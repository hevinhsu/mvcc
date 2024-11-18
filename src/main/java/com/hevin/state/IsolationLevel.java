package com.hevin.state;

public enum IsolationLevel {
	ReadUnCommitted,
	ReadCommitted,
	RepeatableRead,
	Snapshot,
	Serializable
}
