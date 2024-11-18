package com.hevin.dto;

public class Value {

	private int txStartId;	// created by (if operation is update, it will add record with new version)
	private int txEndId;	// deleted by
	private String value;

	public Value(int txStartId, String value) {
		this.txStartId = txStartId;
		this.value = value;
	}

	public int getTxStartId() {
		return txStartId;
	}

	public int getTxEndId() {
		return txEndId;
	}

	public void setTxEndId(int txEndId) {
		this.txEndId = txEndId;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}


}
