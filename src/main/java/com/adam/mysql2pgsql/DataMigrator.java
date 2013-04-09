package com.adam.mysql2pgsql;

import java.io.PrintWriter;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataMigrator {

	private static final Logger LOG = Logger.getLogger(DataMigrator.class.getName());
	private static final long BATCH_SIZE = 10000;
	private Long MAX_QUERY_SIZE = 20_000_000L;
	private final String mysqlPassword;
	private final String mysqlSchema;
	private final String pgsqlPassword;
	private final String pgsqlSchema;
	private final Set<String> onlyMigrateTables;
	private final String mysqlUrl;
	private final String mysqlUser;
	private final String pgsqlUrl;
	private final String pgsqlUser;

	/**
	 * @param mysqlUrl
	 * @param mysqlUser
	 * @param mysqlPassword
	 * @param mysqlSchema
	 * @param pgsqlUrl
	 * @param pgsqlUser
	 * @param pgsqlPassword
	 * @param pgsqlSchema
	 * @param onlyMigrateTables
	 */
	public DataMigrator(
			String mysqlUrl,
			String mysqlUser,
			String mysqlPassword,
			String mysqlSchema,
			String pgsqlUrl,
			String pgsqlUser,
			String pgsqlPassword,
			String pgsqlSchema,
			Set<String> onlyMigrateTables) {
		this.mysqlPassword = mysqlPassword;
		this.mysqlSchema = mysqlSchema;
		this.pgsqlPassword = pgsqlPassword;
		this.pgsqlSchema = pgsqlSchema;
		this.onlyMigrateTables = onlyMigrateTables;
		this.mysqlUrl = mysqlUrl;
		this.mysqlUser = mysqlUser;
		this.pgsqlUrl = pgsqlUrl;
		this.pgsqlUser = pgsqlUser;
	}

	private Connection createMysqlConnection() throws SQLException {
		return DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
	}

	private Connection createPgsqlConnection() throws SQLException {
		return DriverManager.getConnection(pgsqlUrl, pgsqlUser, pgsqlPassword);

	}

	/**
	 * @return A map of all tables (and their estimated sizes) in the current mysql schema, ignoring some obvious ingorables
	 * @throws SQLException
	 */
	private Map<String, Long> getMysqlTableNames() throws SQLException {
		Connection mysqlCon = null;
		PreparedStatement stmt = null;
		Map<String, Long> tableNamesAndSizes = new TreeMap<>();
		try {
			mysqlCon = createMysqlConnection();
			mysqlCon.setCatalog(mysqlSchema);
			stmt = mysqlCon.prepareStatement(""
					+ "SELECT table_name, data_length\n"
					+ "FROM information_schema.tables \n"
					+ "WHERE table_schema = ? \n"
					+ "AND table_type = 'BASE TABLE';"); //Where clause addded to prevent views from appearing in the resultset
			stmt.setString(1, mysqlSchema);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString("table_name");
				Long sizeBytes = rs.getLong("data_length");
				if (tableName == null || tableName.length() == 0) {
					continue;
				}
				String tableNameLc = tableName.toLowerCase();
				if (onlyMigrateTables != null && !onlyMigrateTables.contains(tableNameLc)) {
					continue;
				}
				if (tableNameLc.startsWith("tmp_")
						|| tableNameLc.startsWith("temp_")
						|| tableNameLc.contains("_bak_")
						|| tableNameLc.endsWith("_bak")
						|| tableNameLc.contains("_bck_")
						|| tableNameLc.endsWith("_bck")
						|| tableNameLc.contains("_old_")
						|| tableNameLc.endsWith("_old")) {
					continue;
				}
				tableNamesAndSizes.put(tableName, sizeBytes);
			}
			return tableNamesAndSizes;
		} finally {
			cleanup(mysqlCon);
			cleanup(stmt);
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
	void transferTable(final String tableName, final Long totTableSize) throws SQLException {
		Connection mysqlCon = null;
		Connection pgsqlCon = null;
		try {
			long startTime = System.currentTimeMillis();
			mysqlCon = createMysqlConnection();
			pgsqlCon = createPgsqlConnection();

			PrintWriter writer = System.console().writer();
			NumericColumnRange range = null;
			String numericPkColumn = findNumericPkColumn(mysqlCon, tableName);
			if (numericPkColumn != null && totTableSize > MAX_QUERY_SIZE) {
				PreparedStatement ps = null;
				try {
					ps = mysqlCon.prepareStatement(String.format("SELECT MIN(%s) AS min, MAX(%s) AS max FROM `%s`.`%s`;", numericPkColumn, numericPkColumn, mysqlSchema, tableName));
					ResultSet rs = ps.executeQuery();
					if (rs.next()) {
						range = new NumericColumnRange(numericPkColumn, rs.getLong("min"), rs.getLong("max"));
					}
				} finally {
					cleanup(ps);
				}
			}
			//
			long totRows = 0;
			int batches = 0;
			if (range != null) {
				long nrQueries = Math.round(totTableSize / MAX_QUERY_SIZE);
				long nrRowsPerQuery = (long) Math.ceil(((double) (range.getMax() - range.getMin())) / nrQueries);
				writer.println("Will transfer table " + tableName + " in batches of " + nrRowsPerQuery + " rows per batch. Estim nr batches: " + nrQueries);
				for (long pkValue = range.getMin(); pkValue <= range.getMax(); pkValue += nrRowsPerQuery + 1) {
					totRows += transferTableData(mysqlCon, pgsqlCon, tableName, new NumericColumnRange(numericPkColumn, pkValue, pkValue + nrRowsPerQuery));
					batches++;
					//writer.println(tableName + ": range nr " + ranges + ", " + totRows + ", speed is: " + ((int) (((double) totRows * 1000) / (System.currentTimeMillis() - startTime)) + " r/s"));
				}
			} else {
				totRows = transferTableData(mysqlCon, pgsqlCon, tableName, null);
				batches++;
			}
			long duration = (System.currentTimeMillis() - startTime);
			writer.println("Finished transfering table " + tableName + ": " + totRows + " rows in " + duration + "ms, " + ((int) (((double) totRows * 1000) / duration) + " r/s in " + batches + " batches"));
		} finally {
			cleanup(pgsqlCon);
			cleanup(mysqlCon);
		}
	}

	/**
	 * Transfers data from a mysql table to the corresponding table in pgsql, possibly with a pk range constraint
	 * @param tableName name of the table to transfer
	 * @param range a range constraint, may be null
	 * @return the number of transfered rows
	 * @throws SQLException
	 */
	private int transferTableData(Connection mysqlCon, Connection pgsqlCon, String tableName, NumericColumnRange range) throws SQLException {
		String sql;
		if (range != null) {
			sql = String.format("SELECT * FROM `%s`.`%s` WHERE %s BETWEEN ? AND ?", mysqlSchema, tableName, range.getColName());
		} else {
			sql = String.format("SELECT * FROM `%s`.`%s`", mysqlSchema, tableName);
		}
		PreparedStatement mysqlPs = null;
		PreparedStatement pgsqlPs = null;
		try {
			mysqlPs = mysqlCon.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			if (range != null) {
				mysqlPs.setLong(1, range.getMin());
				mysqlPs.setLong(2, range.getMax());
			}
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
			String insertStmt = generateInsertStatement(pgsqlSchema, tableName, nameByPosition);
			pgsqlPs = pgsqlCon.prepareStatement(insertStmt);
			int totCtr = 0;
			int ctr = 0;
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
					ctr = 0;
				}
			}
			if (ctr > 0) {
				pgsqlPs.executeBatch();
				pgsqlCon.commit();
			}
			return totCtr;
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
		PrintWriter writer = System.console().writer();
		writer.println("Transfer tables called");
		Map<String, Long> mysqlTableNames = getMysqlTableNames();
		ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		Set<Future<?>> futures = new HashSet<>();
		for (final String tableName : mysqlTableNames.keySet()) {
			final Long size = mysqlTableNames.get(tableName);
			Future<?> future = threadPool.submit(new Runnable() {
				@Override
				public void run() {
					try {
						transferTable(tableName, size);
					} catch (SQLException ex) {
						LOG.log(Level.SEVERE, null, ex);
						LOG.log(Level.WARNING, "", ex.getNextException());
					} catch (Throwable th) {
						LOG.log(Level.SEVERE, null, th);
					}
				}
			});
			futures.add(future);
		}
		Object monitor = new Object();
		int originalSize = futures.size();
		while (!futures.isEmpty()) {
			Iterator<Future<?>> futureIter = futures.iterator();
			while (futureIter.hasNext()) {
				Future<?> future = futureIter.next();
				if (future.isDone()) {
					futureIter.remove();
				}
			}
			synchronized (monitor) {
				try {
					monitor.wait(1000);
				} catch (InterruptedException ex) {
				}
			}
			int done = originalSize - futures.size();
			writer.println(done + " tables done, " + futures.size() + " out of " + originalSize + " tables remaining...");
		}
		threadPool.shutdown();
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
			colNames.append("\"").append(colName.toLowerCase()).append("\"");
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
			case Types.BIT: {
				boolean value = mysqlRs.getBoolean(colName);
				if (mysqlRs.wasNull()) {
					pgsqlPs.setNull(position, Types.BIT);
				} else {
					pgsqlPs.setBoolean(position, value);
				}
				break;
			}
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT: {
				int value = mysqlRs.getInt(colName);
				if (mysqlRs.wasNull()) {
					pgsqlPs.setNull(position, Types.INTEGER);
				} else {
					pgsqlPs.setInt(position, value);
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

	private String findNumericPkColumn(Connection mysqlCon, String tableName) throws SQLException {
		PreparedStatement ps = null;
		try {
			ps = mysqlCon.prepareStatement(""
					+ "SHOW COLUMNS \n"
					+ "FROM `" + mysqlSchema + "`.`" + tableName + "` \n"
					+ "WHERE `Null` = 'NO' \n"
					+ "AND `Key` IN ('PRI') \n"
					+ "AND (`Type` LIKE 'bigint%' OR `Type` LIKE 'int%')"
					+ ";");
			ResultSet rs = ps.executeQuery();
			String colName;
			if (rs.next()) {
				colName = rs.getString("Field");
			} else {
				colName = null;
			}
			return colName;
		} finally {
			cleanup(ps);
		}
	}
}
