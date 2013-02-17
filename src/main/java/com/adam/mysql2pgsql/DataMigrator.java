package com.adam.mysql2pgsql;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataMigrator implements AutoCloseable {

	private static final long BATCH_SIZE = 10000;
	private final Connection mysqlCon;
	private final Connection pgsqlCon;
	private final String schemaName;

	public DataMigrator(String mysqlUrl, String mysqlUser, String mysqlPassword, String pgsqlUrl, String pgsqlUser, String pgsqlPassword, String schemaName) throws SQLException {
		this.mysqlCon = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
		this.pgsqlCon = DriverManager.getConnection(pgsqlUrl, pgsqlUser, pgsqlPassword);
		this.schemaName = schemaName;
	}

	/**
	 * First, get a list of ignored table names, for the given connection
	 */
	private Set<String> getMysqlTableNames() throws SQLException {
		mysqlCon.setCatalog(schemaName);
		Set<String> tableNames = new TreeSet<>();
		String SQL = "SHOW TABLE STATUS";
		try (PreparedStatement stmt = mysqlCon.prepareStatement(SQL)) {
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString(1);
				if (tableName == null || tableName.length() == 0) {
					continue;
				}
				String tableNameLc = tableName.toLowerCase();
				if (false && tableNameLc.startsWith("tmp_")
						|| tableNameLc.startsWith("temp_")
						|| tableNameLc.contains("_bak_")
						|| tableNameLc.endsWith("_bak")
						|| tableNameLc.contains("_bck_")
						|| tableNameLc.endsWith("_bck")
						|| tableNameLc.contains("_old_")
						|| tableNameLc.endsWith("_old")) {
					continue;
				}
				tableNames.add(tableName);
			}
			return tableNames;
		}
	}

	private void cleanup(Statement stmt) {
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (Exception e) {
		}
	}

	private void cleanup(Connection con) {
		try {
			if (con != null) {
				con.close();
			}
		} catch (Exception e) {
		}
	}

	/**
	 * Transfers all data from the specified table in mysql to postgres
	 * @param tableName the name of the table to transfer
	 * @throws SQLException
	 */
	private void transferTable(String tableName) throws SQLException {
		System.out.println("Transfering data for table " + tableName);
		PreparedStatement mysqlPs = null;
		PreparedStatement pgsqlPs = null;
		try {
			mysqlPs = mysqlCon.prepareStatement("select * from " + "`" + schemaName + "`.`" + tableName + "`");
			ResultSet mysqlRs = mysqlPs.executeQuery();
			ResultSetMetaData metaData = mysqlRs.getMetaData();
			int columnCount = metaData.getColumnCount();
			Map<Integer, Integer> typeByPosition = new TreeMap<>();
			Map<Integer, String> nameByPosition = new TreeMap<>();
			for (int i = 1; i <= columnCount; i++) {
				typeByPosition.put(i, metaData.getColumnType(i));
				nameByPosition.put(i, metaData.getColumnLabel(i));
			}
			pgsqlCon.setAutoCommit(false);
			String insertStmt = generateInsertStatement(schemaName, tableName, nameByPosition);
			pgsqlPs = pgsqlCon.prepareStatement(insertStmt);
			int totCtr = 0;
			int ctr = 0;
			long startTime = System.currentTimeMillis();
			while (mysqlRs.next()) {
				for (int position = 1; position <= columnCount; position++) {
					int type = typeByPosition.get(position);
					String colName = nameByPosition.get(position);
					transferColumn(type, position, mysqlRs, colName, pgsqlPs);

				}
				pgsqlPs.addBatch();
				ctr++;
				totCtr++;
				if (ctr % BATCH_SIZE == 0) {
					pgsqlPs.executeBatch();
					pgsqlCon.commit();
					System.out.println(tableName + ": interCtr = " + totCtr + ", speed is: " + ((int)(((double) totCtr * 1000) / (System.currentTimeMillis() - startTime)) + " r/s"));
					ctr = 0;
				}
			}
			if (ctr > 0) {
				pgsqlPs.executeBatch();
				System.out.println(tableName + ": totCtr = " + totCtr + ", speed is: " + ((int)(((double) totCtr * 1000) / (System.currentTimeMillis() - startTime)) + " r/s"));
				pgsqlCon.commit();
			}
		} finally {
			cleanup(mysqlPs);
			cleanup(pgsqlPs);
		}
	}

	/**
	 * Tranfers all data from each table in the mysql DB to the postgres DB
	 * @throws SQLException
	 */
	public void transferTables() throws SQLException {
		System.out.println("Transfer tables called");
		Set<String> mysqlTableNames = getMysqlTableNames();
		for (String tableName : mysqlTableNames) {
			transferTable(tableName);
		}
	}

	/**
	 * Generates a full insert statement for a table, eg INSERT INTO "schema"."table"("col1", "col2") VALUES(?,?)
	 * @param schemaName the db schema
	 * @param tableName the name of the table
	 * @param columnNameByPosition a map with all columns for the table
	 * @return the SQL insert string
	 */
	private String generateInsertStatement(String schemaName, String tableName, Map<Integer, String> columnNameByPosition) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO \"").append(schemaName).append("\".\"").append(tableName).append("\"");
		Iterator<Integer> iterator = columnNameByPosition.keySet().iterator();
		StringBuilder colNames = new StringBuilder();
		StringBuilder questionMarks = new StringBuilder();

		while (iterator.hasNext()) {
			Integer colPosition = iterator.next();
			String colName = columnNameByPosition.get(colPosition);
			colNames.append("\"").append(colName).append("\"");
			questionMarks.append("?");
			if (iterator.hasNext()) {
				colNames.append(", ");
				questionMarks.append(", ");
			}
		}
		sb.append("(").append(colNames).append(") VALUES (").append(questionMarks).append(")");
		return sb.toString();
	}

	private void transferColumn(int type, int position, ResultSet mysqlRs, String colName, PreparedStatement pgsqlPs) throws SQLException {
		switch (type) {
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.REAL:
			case Types.FLOAT: {
				double value = mysqlRs.getDouble(colName);
//								System.out.println(tableName + "." + colName + ": Getting from mysql: " + type + " " + metaData.getColumnTypeName(position) + " setting in pgsql: decimal");
				if (mysqlRs.wasNull()) {
					pgsqlPs.setNull(position, Types.DECIMAL);
				} else {
					pgsqlPs.setDouble(position, value);
				}
				break;
			}
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT:
			case Types.BIT: {
				long value = mysqlRs.getInt(colName);
				if (mysqlRs.wasNull()) {
					pgsqlPs.setNull(position, Types.INTEGER);
				} else {
					pgsqlPs.setLong(position, value);
				}
				break;
			}
			case Types.BIGINT: {
				long value = mysqlRs.getLong(colName);
				if (mysqlRs.wasNull()) {
					pgsqlPs.setNull(position, Types.BIGINT);
				} else {
					pgsqlPs.setLong(position, value);
				}
				break;
			}
			case Types.DATE:
			case Types.TIMESTAMP:
			case Types.TIME: {
				try {
					Timestamp value = mysqlRs.getTimestamp(colName);
					if (mysqlRs.wasNull()) {
						pgsqlPs.setNull(position, Types.TIMESTAMP);
					} else {
						pgsqlPs.setTimestamp(position, value);
					}
				} catch (SQLException e) {
					String message = e.getMessage();
					if (message.startsWith("Value '") && message.endsWith("' can not be represented as java.sql.Timestamp")) {
						pgsqlPs.setTimestamp(position, new Timestamp(0L));
					} else {
						throw e;
					}
				}
				break;
			}
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.CHAR: {
				String value = mysqlRs.getString(colName);
				if (value != null) {
					try {
						String removeChars = new String(new byte[]{(byte) 0x00}, "utf-8");
						//Mysql sometimes has an initial 0x00 byte which is not an allowed utf-8 character. Strip it
						value = value.replace(removeChars, "");
					} catch (UnsupportedEncodingException ex) {
						Logger.getLogger(DataMigrator.class.getName()).log(Level.SEVERE, null, ex);
					}
				}
				if (mysqlRs.wasNull()) {
					pgsqlPs.setNull(position, Types.VARCHAR);
				} else {
					pgsqlPs.setString(position, value);
				}
				break;
			}
			case Types.LONGVARBINARY:
			case Types.BLOB:
			case Types.VARBINARY:
			case Types.BINARY: {
				byte[] value = mysqlRs.getBytes(colName);
				if (mysqlRs.wasNull()) {
					pgsqlPs.setNull(position, type);
				} else {
					pgsqlPs.setBytes(position, value);
				}
				break;
			}
			default:
				throw new SQLException("Don't know how to handle type of " + type + " " + mysqlRs.getMetaData().getColumnTypeName(position));
		}
	}

	@Override
	public void close() throws IOException {
		cleanup(mysqlCon);
		cleanup(pgsqlCon);
	}
}
