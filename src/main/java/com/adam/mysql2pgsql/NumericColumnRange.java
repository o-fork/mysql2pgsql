package com.adam.mysql2pgsql;

/**
 * Util class for defining a column and a range
 * @author adam
 */
public class NumericColumnRange {

	private final String colName;
	private final long min;
	private final long max;

	public NumericColumnRange(String colName, long min, long max) {
		this.colName = colName;
		this.min = min;
		this.max = max;
	}

	public String getColName() {
		return colName;
	}

	public long getMin() {
		return min;
	}

	public long getMax() {
		return max;
	}
}
