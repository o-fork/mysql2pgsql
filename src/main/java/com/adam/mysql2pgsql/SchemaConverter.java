package com.adam.mysql2pgsql;

import static java.lang.String.format;
import static java.util.regex.Pattern.compile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

public class SchemaConverter {

	final String mysqlSchema;
	final String mysqlHost;
	final Integer mysqlPort;
	final String mysqlUser;
	final String mysqlPassword;
	final String pgsqlSchema;
	private List<String> dumpRows;
	private final Set<String> onlyMigrateTables;

	/**
	 * @param mysqlSchema
	 * @param mysqlHost
	 * @param mysqlPort
	 * @param mysqlUser
	 * @param mysqlPassword
	 * @param pgsqlSchema
	 * @param onlyMigrateTables
	 */
	public SchemaConverter(
			String mysqlSchema,
			String mysqlHost,
			Integer mysqlPort,
			String mysqlUser,
			String mysqlPassword,
			String pgsqlSchema,
			Set<String> onlyMigrateTables) {
		this.pgsqlSchema = pgsqlSchema;
		this.mysqlSchema = mysqlSchema;
		this.mysqlHost = mysqlHost;
		this.mysqlPort = mysqlPort;
		this.mysqlUser = mysqlUser;
		this.mysqlPassword = mysqlPassword;
		this.onlyMigrateTables = onlyMigrateTables;
	}

	/**
	 * Uses mysqldump to dump entire db metadata from mysql. There are alternative ways of doing this, such as
	 * mysql-specific SQL-queries, but this turned out to be the fastest method.
	 * @throws IOException
	 */
	@SuppressWarnings("SleepWhileInLoop")
	protected void generateMysqlDumpData() throws IOException {
		List<String> args = new LinkedList<>();
		args.add("mysqldump");
		args.add("--skip-comments");
		args.add("--no-data");
		args.add("--host=" + mysqlHost);
		args.add("--port=" + String.valueOf(mysqlPort));
		args.add("--user=" + mysqlUser);
		args.add("--password=" + mysqlPassword);
		args.add("--default-character-set=utf8mb4");
		args.add("--single-transaction");
		args.add(mysqlSchema);
		if (onlyMigrateTables != null) {
			for (String table : onlyMigrateTables) {
				args.add(table);
			}
		}
		ProcessBuilder pb = new ProcessBuilder(args);
		pb.redirectErrorStream(true);
		System.console().printf("Generating schema dump...");
		Process process = pb.start();
		InputStream is = process.getInputStream();
		final BufferedReader br = new BufferedReader(new InputStreamReader(is));
		List<String> lines = new ArrayList<>();
		String line;
		while ((line = br.readLine()) != null) {
			if (line.isEmpty() || "\n".equals(line)) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException ex) {
				}
				continue;
			}
			lines.add(line);
		}
		try {
			int exitCode = process.waitFor();
			if (exitCode != 0) {
				System.console().printf("Got exit code %d from subprocess", exitCode);
				throw new IOException(lines.toString());
			}
		} catch (InterruptedException ex) {
		}
		this.dumpRows = lines;
	}
	private List<TableMetaData> tables;

	protected void parseSchemaDump() throws ParseException {
		TableMetaData tableMetaData = null;
		this.tables = new ArrayList<>();
		Matcher m;
		for (String tmp : dumpRows) {
			String line = tmp;
			line = line.trim();
			if (line.endsWith(",")) {
				line = line.substring(0, line.length() - 1);
			}
			//Create table
			m = compile("CREATE TABLE `([^`]+)` \\(").matcher(line);
			if (m.matches()) {
				String tableName = m.group(1);
				tableMetaData = new TableMetaData(tableName);
				continue;
			}
			//Closing parenthesis
			m = compile("^\\) ENGINE=.*$").matcher(line);
			if (m.matches()) {
				if(tableMetaData != null && (onlyMigrateTables == null || onlyMigrateTables.contains(tableMetaData.getTableName().toLowerCase()))){
					tables.add(tableMetaData);
				}
				tableMetaData = null;
				continue;
			}
			if (tableMetaData == null) {
				//Noy interested
				//TODO: Parse extra table metadata, such as partitioning info etc
				continue;
			}
			//Indices
			if (line.startsWith("KEY")) {
				convertIndexInstruction(tableMetaData, line);
				continue;
			}
			// CONSTRAINT "fk_constraint_name" FOREIGN KEY ("col_name") REFERENCES "ref_table_name" ("ref_col_name") ON UPDATE NO ACTION
			m = compile("^CONSTRAINT `(\\S+)` FOREIGN KEY \\(`(\\S+)`\\) REFERENCES `(\\S+)` \\(`(\\S+)`\\)[A-Z\\s]*$").matcher(line);
			if (m.matches()) {
				tableMetaData.addConstraint(format("ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT \"%s\" FOREIGN KEY (\"%s\") REFERENCES \"%s\" (\"%s\") ON UPDATE NO ACTION ON DELETE NO ACTION", pgsqlSchema, tableMetaData.getTableName(), m.group(1), m.group(2), m.group(3), m.group(4)));
				continue;
			}
			//Unique constraints
			//from: UNIQUE KEY `ix_name` (`col1`,`col2`,`col3`,`col4`) USING BTREE
			//to:   ALTER TABLE "schema_name"."table_name" ADD CONSTRAINT "ix_name" UNIQUE ("col1", "col2", "col3", "col4");
			m = compile("^UNIQUE KEY `(\\S+)` \\((\\S+)\\)[^\\(]*$").matcher(line);
			if (m.matches()) {
				tableMetaData.addConstraint(format("ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT \"%s_%s\" UNIQUE (%s)", pgsqlSchema, tableMetaData.getTableName(), tableMetaData.getTableName(), m.group(1), m.group(2).replaceAll("`", "\"")));
				continue;
			}
			//Primary keys
			//from: PRIMARY KEY (`col1`,`col2`) USING BTREE
			//to  : ALTER TABLE "schema_name"."table_name" ADD CONSTRAINT "table_name_pkey" PRIMARY KEY ("col1", "col2");
			m = compile("^PRIMARY KEY \\((\\S+)\\)[^\\(]*$").matcher(line);
			if (m.matches()) {
				tableMetaData.addPk(format("ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT \"%s_pkey\" PRIMARY KEY (%s)", pgsqlSchema, tableMetaData.getTableName(), tableMetaData.getTableName(), m.group(1).replaceAll("`", "\"")));
				continue;
			}
			//
			//Now we know that the row is a column definition. Find it's name:
			m = compile("`(\\S+)`.*").matcher(line);
			if (!m.matches()) {
				throw new ParseException("Could not find the column name from line: " + line, -1);
			}
			String columnName = m.group(1).toLowerCase();
			line = line.substring(0, m.start(1)) + columnName + line.substring(m.end(1));
			//Extract possible comments
			m = compile("^.* (COMMENT) '(.*)'$").matcher(line);
			if (m.matches()) {
				tableMetaData.addComment(format("COMMENT ON COLUMN \"%s\".\"%s\".\"%s\" IS '%s';", pgsqlSchema, tableMetaData.getTableName(), columnName, m.group(2)));
				line = line.substring(0, m.start(1) - 1) + line.substring(m.end(2) + 1);
			}

			//Remove some not needed charset definitions:
			line = line.replace(" COLLATE=utf8mb4_unicode_ci", "");
			line = line.replace(" CHARACTER SET latin1", "");
			line = line.replace(" CHARACTER SET utf8mb4", "");
			line = line.replace(" COLLATE utf8mb4_unicode_ci", "");
			//Remove autoupdating timestamps
			line = line.replace(" DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", "");


			//Some data type conversion:
			line = line.replace(" datetime NOT NULL DEFAULT '0000-00-00 00:00:00'", " timestamp NOT NULL");
			line = line.replace(" datetime", " timestamp");
			if (line.contains("AUTO_INCREMENT")) {
				convertAutoIncrementInstruction(tableMetaData, line);
				continue;
			}

			line = line.replaceFirst("^`(\\S+)` tinyint\\(1\\)([A-Z ]*) DEFAULT '0'$", "`$1` boolean$2 DEFAULT false");
			line = line.replaceFirst("^`(\\S+)` tinyint\\(1\\)([A-Z ]*) DEFAULT '1'$", "`$1` boolean$2 DEFAULT true");
			line = line.replace(" tinyint(1)", " boolean");

			line = line.replaceFirst("^`(\\S+)` bit\\(1\\)([A-Z ]*) DEFAULT b'0'$", "`$1` boolean$2 DEFAULT false");
			line = line.replaceFirst("^`(\\S+)` bit\\(1\\)([A-Z ]*) DEFAULT b'1'$", "`$1` boolean$2 DEFAULT true");
			line = line.replace(" bit(1)", " boolean");

			line = line.replaceFirst(" int\\([0-9]*\\)", " integer");
			line = line.replaceFirst(" bigint\\([0-9]*\\)", " bigint");
			line = line.replaceFirst(" tinyint\\([0-9]*\\)", " smallint");
			line = line.replaceFirst(" smallint\\([0-9]*\\)", " smallint");

			line = line.replace(" double", " double precision");
			line = line.replace(" tinytext", " text");
			line = line.replace(" mediumtext", " text");
			line = line.replace(" longtext", " text");
			line = line.replace(" tinyblob", " bytea");
			line = line.replace(" mediumblob", " bytea");
			line = line.replace(" longblob", " bytea");
			line = line.replace(" blob", " bytea");

			line = line.replace(" unsigned", " ");
			// Special case for "inactivevated" columns with 0 length strings (which postgresql won't allow):
			line = line.replace(" varchar(0)", " character varying(1)");
			line = line.replaceFirst(" varchar\\(([0-9]*)\\)", " character varying($1)");

			tableMetaData.addColDefinition(line.replace("`", "\""));

		}
	}

	private static String listToString(List<String> strings) {
		String retStr = "";
		for (int i = 0; i < strings.size(); i++) {
			retStr += strings.get(i);
			if (i < strings.size() - 1) {
				retStr += ",";
			}
		}
		return retStr;
	}

	File generatePostgresTableDefinitionFile(String pgsqlUser) throws IOException {
		File file = File.createTempFile(pgsqlSchema + "_table_definition", ".sql");
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			for (TableMetaData tableMetaData : tables) {
				writer.write(tableMetaData.generateCreateTableStatement(pgsqlSchema));
				writer.write('\n');
			}
			writer.flush();
		}
		return file;
	}

	File generatePostgresSchemaDefinitionFile(String pgsqlUser) throws IOException {
		File file = File.createTempFile(pgsqlSchema + "_schema_definition", ".sql");
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			writer.write("DROP SCHEMA IF EXISTS " + pgsqlSchema + " CASCADE;\n");
			writer.write("CREATE SCHEMA " + pgsqlSchema + " AUTHORIZATION " + pgsqlUser + ";\n");
			writer.write('\n');
			writer.flush();
		}
		return file;
	}

	File generatePostgresIndexAndConstraintsFile() throws IOException {
		File file = File.createTempFile(pgsqlSchema + "_ix_constraints_definition", ".sql");
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			for (TableMetaData tableMetaData : tables) {
				String cons = tableMetaData.generateConstraintsStatement();
				if (cons != null && !cons.isEmpty()) {
					writer.write(cons);
					writer.write('\n');
				}
				String idx = tableMetaData.generateIndicesStatement();
				if (idx != null && !idx.isEmpty()) {
					writer.write(idx);
					writer.write('\n');
				}
			}
			writer.flush();
		}
		return file;
	}

	File generatePostgresPkDefFile() throws IOException {
		File file = File.createTempFile(pgsqlSchema + "_pk_definition", ".sql");
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			for (TableMetaData tableMetaData : tables) {
				String pk = tableMetaData.generatePkStatement();
				if (pk != null && !pk.isEmpty()) {
					writer.write(pk);
					writer.write('\n');
				}
			}
			writer.flush();
		}
		return file;
	}

	File generatePostSqlFile() throws IOException {
		File file = File.createTempFile(pgsqlSchema + "_post_sqls", ".sql");
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			for (TableMetaData tableMetaData : tables) {
				String postSql = tableMetaData.generatePostSqls();
				if (postSql != null && !postSql.isEmpty()) {
					writer.write(postSql);
					writer.write('\n');
				}
			}
			writer.flush();
		}
		return file;
	}

	private void convertIndexInstruction(TableMetaData tableMetaData, String line) throws ParseException {
		//from: KEY `ix_name` (`col1`,`col2`,`col3`,`col4`) USING BTREE
		//to:   CREATE INDEX "schema_name_table_name" ON "schema_name"."table_name" ("col1", "col2", "col3", "col4");
		Matcher m = compile("^KEY `(\\S+)` \\((.*)\\)[^\\(]*$").matcher(line);
		if (m.matches()) {
			String[] cols = m.group(2).split(",");
			List<String> colsList = new LinkedList<>();
			for (String col : cols) {
				Matcher m2 = compile("`(\\S+)`(\\([0-9]+\\))?").matcher(col);
				if (m2.matches()) {
					String numeric = m2.group(2);
					if (numeric != null && !numeric.isEmpty()) {
						colsList.add("left(\"" + m2.group(1) + "\", " + numeric.substring(1, numeric.length() - 1) + ")");
					} else {
						colsList.add("\"" + m2.group(1) + "\"");
					}
				}
			}
			tableMetaData.addIndex(format("CREATE INDEX \"%s_%s\" ON \"%s\".\"%s\" (%s)", tableMetaData.getTableName(), m.group(1), pgsqlSchema, tableMetaData.getTableName(), listToString(colsList)));
		} else {
			throw new ParseException("Could not parse index row: " + line, -1);
		}
	}

	void convertAutoIncrementInstruction(TableMetaData tableMetaData, String line) throws ParseException {
		Matcher m = compile("^`(\\S+)` (big)?int\\([0-9]*\\) (unsigned )?NOT NULL AUTO_INCREMENT").matcher(line);
		if (m.matches()) {
			tableMetaData.addColDefinition("\"" + m.group(1) + "\" " + (m.group(2) != null ? m.group(2) : "") + "serial");
			tableMetaData.addPostSQL(format(
					"SELECT setval('\"%s\".\"%s_%s_seq\"', (select coalesce(max(\"%s\"), 0)+1 from \"%s\".\"%s\"))",
					pgsqlSchema,
					tableMetaData.getTableName(),
					m.group(1),
					m.group(1),
					pgsqlSchema,
					tableMetaData.getTableName()));
		} else {
			throw new ParseException("Could not parse auto increment row: " + line, -1);
		}
	}
}
