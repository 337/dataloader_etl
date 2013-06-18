package com.xingcloud.dataloader.hbase.table;

import java.util.Arrays;

/**
 * Author: mulisen
 * Date:   12-8-2
 */
public class EventMeta {

	/**
	 * in database, status = Status.xxx.ordinal:
	 * 	normal = 0
	 * 	deleted = 1
	 * in memory:
	 * 	exceeding = 2, 3, so
	 *
	 * DO NOT CHANGE the order of Status!
	 *
	 */
	public static enum Status{normal, deleted, exceedingTotal, exceedingLevel}

	String[] key;
	long lastUpatetime;


	/**
	 * ObjectId for storage
	 */
	Object _id;

	/**
	 *
	 */
	Status status = Status.normal;
	/**
	 * when status = exceedingLevel, level indicates level of exceeding
	 */
	int level = -1;

	public EventMeta(String[] key, long lastUpatetime, Object _id) {
		this.key = key;
		this.lastUpatetime = lastUpatetime;
		this._id = _id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		EventMeta eventMeta = (EventMeta) o;

		if (!Arrays.equals(key, eventMeta.key)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return key != null ? Arrays.hashCode(key) : 0;
	}

}
