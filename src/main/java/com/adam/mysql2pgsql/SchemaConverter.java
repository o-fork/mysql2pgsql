package com.adam.mysql2pgsql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;

import static java.lang.String.format;
import static java.util.regex.Pattern.compile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.ParseException;

public class SchemaConverter {

	final String schemaName;
	final String mysqlHost;
	final Integer mysqlPort;
	final String mysqlUser;
	final String mysqlPassword;

	public SchemaConverter(String schemaName, String mysqlHost, Integer mysqlPort, String mysqlUser, String mysqlPassword) {
		this.schemaName = schemaName;
		this.mysqlHost = mysqlHost;
		this.mysqlPort = mysqlPort;
		this.mysqlUser = mysqlUser;
		this.mysqlPassword = mysqlPassword;
	}

	/**
	 * Uses mysqldump to dump entire db metadata from mysql. There are alternative ways of doing this, such as
	 * mysql-specific SQL-queries, but this turned out to be the fastest method.
	 * @return The full metadata test. One list item for each line produced by mysqldump
	 * @throws IOException
	 */
	@SuppressWarnings("SleepWhileInLoop")
	protected List<String> generateMysqlDumpData() throws IOException {
		List<String> args = new LinkedList<>();
		args.add("/usr/bin/mysqldump");
		args.add("--skip-comments");
		args.add("--no-data");
		args.add("--host=" + mysqlHost);
		args.add("--port=" + String.valueOf(mysqlPort));
		args.add("--user=" + mysqlUser);
		args.add("--password=" + mysqlPassword);
		args.add("--default-character-set=utf8mb4");
		args.add("--single-transaction");
		args.add(schemaName);
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
				System.err.println("Got exit code " + exitCode + " from subprocess");
				throw new IOException(lines.toString());
			}
		} catch (InterruptedException ex) {
		}
		return lines;
	}

	protected List<TableMetaData> parseSchemaDump(List<String> lines) throws ParseException {
		TableMetaData tableMetaData = null;
		List<TableMetaData> tables = new ArrayList<>();
		Matcher m;
		for (String tmp : lines) {
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
				tables.add(tableMetaData);
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
				tableMetaData.addConstraint(format("ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT \"%s\" FOREIGN KEY (\"%s\") REFERENCES \"%s\" (\"%s\") ON UPDATE NO ACTION ON DELETE NO ACTION", schemaName, tableMetaData.getTableName(), m.group(1), m.group(2), m.group(3), m.group(4)));
				continue;
			}
			//Unique constraints
			//from: UNIQUE KEY `ix_name` (`col1`,`col2`,`col3`,`col4`) USING BTREE
			//to:   ALTER TABLE "schema_name"."table_name" ADD CONSTRAINT "ix_name" UNIQUE ("col1", "col2", "col3", "col4");
			m = compile("^UNIQUE KEY `(\\S+)` \\((\\S+)\\)[^\\(]*$").matcher(line);
			if (m.matches()) {
				tableMetaData.addConstraint(format("ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT \"%s_%s\" UNIQUE (%s)", schemaName, tableMetaData.getTableName(), tableMetaData.getTableName(), m.group(1), m.group(2).replaceAll("`", "\"")));
				continue;
			}
			//Primary keys
			//from: PRIMARY KEY (`col1`,`col2`) USING BTREE
			//to  : ALTER TABLE "schema_name"."table_name" ADD CONSTRAINT "table_name_pkey" PRIMARY KEY ("col1", "col2");
			m = compile("^PRIMARY KEY \\((\\S+)\\)[^\\(]*$").matcher(line);
			if (m.matches()) {
				tableMetaData.addPk(format("ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT \"%s_pkey\" PRIMARY KEY (%s)", schemaName, tableMetaData.getTableName(), tableMetaData.getTableName(), m.group(1).replaceAll("`", "\"")));
				continue;
			}
			//
			//Now we know that the row is a column definition. Find it's name:
			m = compile("`(\\S+)`.*").matcher(line);
			if (!m.matches()) {
				throw new ParseException("Could not find the column name from line: " + line, -1);
			}
			String columnName = m.group(1);

			//Extract possible comments
			m = compile("^.* (COMMENT) '(.*)'$").matcher(line);
			if (m.matches()) {
				tableMetaData.addComment(format("COMMENT ON COLUMN \"%s\".\"%s\".\"%s\" IS '%s';", schemaName, tableMetaData.getTableName(), columnName, m.group(2)));
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
			//int with auto inc -> to serial
			line = line.replaceFirst(" int\\([0-9]*\\) (unsigned )?NOT NULL AUTO_INCREMENT", " serial");
			// bigint with auto inc -> to bigserial
			line = line.replaceFirst(" bigint\\([0-9]*\\) (unsigned )?NOT NULL AUTO_INCREMENT", " bigserial");

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
		return tables;
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

	File generatePostgresTableDefinitionFile(String schemaName, String pgsqlUser, List<TableMetaData> tables) throws IOException {
		File file = File.createTempFile(schemaName + "_schema_definition", ".sql");
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			writer.write("DROP SCHEMA IF EXISTS " + schemaName + " cascade;\n");
			writer.write("CREATE SCHEMA " + schemaName + " authorization " + pgsqlUser + ";\n");
			writer.write('\n');
			for (TableMetaData tableMetaData : tables) {
				writer.write(tableMetaData.generateCreateTableStatement(schemaName));
				writer.write('\n');
			}
			writer.flush();
		}
		return file;
	}

	File generatePostgresIndexAndConstraintsFile(String schemaName, String pgsqlUser, List<TableMetaData> tables) throws IOException {
		File file = File.createTempFile(schemaName + "_ix_constraints_definition", ".sql");
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

	File generatePostgresPkDefFile(String schemaName, String pgsqlUser, List<TableMetaData> tables) throws IOException {
		File file = File.createTempFile(schemaName + "_pk_definition", ".sql");
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
			tableMetaData.addIndex(format("CREATE INDEX \"%s_%s\" ON \"%s\".\"%s\" (%s)", tableMetaData.getTableName(), m.group(1), schemaName, tableMetaData.getTableName(), listToString(colsList)));
		} else {
			throw new ParseException("Could not parse index row: " + line, -1);
		}
	}
}
