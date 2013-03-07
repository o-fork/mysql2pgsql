package com.adam.mysql2pgsql;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Set;
import java.util.TreeSet;

public class App {

	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, ParseException {
		Console console = System.console();
		if (console == null) {
			System.err.println("Need a console for interactivity");
			System.exit(1);
			return;
		}
		PrintWriter writer = console.writer();
		if (args == null || args.length < 9) {
			writer.append("Missing arguments!\n");
			writer.append("Required arguments missing!\n");
			writer.append("Please invoce the script with the following arguments: \n");
			writer.append("\n");
			writer.append("\tmysqlhost mysqlport mysqluser mysqlschema pgsqlhost pgsqlport pgsqldb pgsqluser pgsqlschema [tablea, tableb]\n");
			writer.append("\n");
			writer.append("If any table specified, only that/those specific tables will be migrated. Otherwise all tables in the entire schema \n");
			writer.append("\n");
			writer.flush();
			System.exit(1);
		}
		String mysqlHost = args[0];
		Integer mysqlPort = Integer.parseInt(args[1]);
		String mysqlUser = args[2];
		writer.println("Please provide password for the mysql instance " + mysqlUser + "@" + mysqlHost + ":" + mysqlPort + ":");
		String mysqlSchema = args[3];
		String mysqlPassword = new String(console.readPassword());

		String pgsqlHost = args[4];
		Integer pgsqlPort = Integer.parseInt(args[5]);
		String pgsqlDb = args[6];
		String pgsqlUser = args[7];
		writer.println("Please provide password for the pgsql instance " + pgsqlUser + "@" + pgsqlHost + ":" + pgsqlPort + ":");
		String pgsqlSchema = args[8];
		String pgsqlPassword = new String(console.readPassword());

		String mysqlUrl = "jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/";
		DriverManager.registerDriver((java.sql.Driver) Class.forName("com.mysql.jdbc.Driver").newInstance());

		String pgsqlUrl = "jdbc:postgresql://" + pgsqlHost + ":" + pgsqlPort + "/" + pgsqlDb;
		DriverManager.registerDriver((java.sql.Driver) Class.forName("org.postgresql.Driver").newInstance());

		Set<String> onlyMigrateTables = null;
		if (args != null && args.length > 9) {
			onlyMigrateTables = new TreeSet<>();
			for (int i = 9; i < args.length; i++) {
				onlyMigrateTables.add(args[i].toLowerCase());
			}
			writer.println("Will only migrate tables: " + onlyMigrateTables);
		} else {
			writer.println("Will migrate all tables in the schema");
		}

		SchemaConverter schemaConverter = new SchemaConverter(mysqlSchema, mysqlHost, mysqlPort, mysqlUser, mysqlPassword, pgsqlSchema, onlyMigrateTables);
		PSQLExecutor psqle = new PSQLExecutor(pgsqlHost, pgsqlDb, pgsqlPort, pgsqlUser, pgsqlPassword);

		//Parse mysql schema
		writer.println("Dumping and parsing mysql schema...");
		schemaConverter.generateMysqlDumpData();
		schemaConverter.parseSchemaDump();
		writer.println("Done\n");

		boolean recreateSchema = promptIfSchemaRecreation(pgsqlSchema);
		if (recreateSchema) {
			writer.println("Deleting and creating schema in postgres...");
			File postgresSchemaDefFile = schemaConverter.generatePostgresSchemaDefinitionFile(pgsqlUser);
			psqle.executeFile(postgresSchemaDefFile);
			writer.println("Done\n");
		}

		writer.println("Deleting and creating tables in the postgres schema...");
		//Apply converted schema definition in postgres
		File postgresTableDefFile = schemaConverter.generatePostgresTableDefinitionFile(pgsqlUser);
		writer.println(postgresTableDefFile.getAbsolutePath());
		psqle.executeFile(postgresTableDefFile);
		writer.println("Done\n");

		//Migrate all data to the new schema
		writer.println("Migrating actual data from mysql to posgres...");
		try (DataMigrator dataMigrator = new DataMigrator(mysqlUrl, mysqlUser, mysqlPassword, mysqlSchema, pgsqlUrl, pgsqlUser, pgsqlPassword, pgsqlSchema, onlyMigrateTables)) {
			dataMigrator.transferTables();
		} catch (SQLException sqle) {
			sqle.printStackTrace(System.out);
			SQLException nextException = sqle.getNextException();
			if (nextException != null) {
				sqle.getNextException().printStackTrace(System.out);
			}
			throw sqle;
		}
		writer.println("Done\n");

		//Apply all constraints and indices
		writer.println("Applying pk constraints...");
		File postgresPkDefFile = schemaConverter.generatePostgresPkDefFile();
		psqle.executeFile(postgresPkDefFile);
		writer.println("Done.\n");

		writer.println("Applying fk constraints and creating indices...");
		File postgresIdxAndConstraintsFile = schemaConverter.generatePostgresIndexAndConstraintsFile();
		writer.println(postgresIdxAndConstraintsFile.getAbsolutePath());
		psqle.executeFile(postgresIdxAndConstraintsFile);
		writer.println("Done\n");

		writer.println("Running post SQLs: Updaing sequences to current increment value...");
		File postSqlFile = schemaConverter.generatePostSqlFile();
		writer.println(postSqlFile.getAbsolutePath());
		psqle.executeFile(postSqlFile);
		writer.println("Done\n");

		//All done
		writer.println("All done");

	}

	private static boolean promptIfSchemaRecreation(String pgsqlSchema) {
		Console console = System.console();
		PrintWriter writer = console.writer();
		//Security check
		writer.println("Drop existing schema \"" + pgsqlSchema + "\" and recreate it?");
		writer.println("Please verify that this is what's intended. Note: for both 'Y' and 'N', each individual TABLE will still recreated");
		writer.println("\t'Y' Yes");
		writer.println("\t'N' No. Continuing without dropping existing schema.");
		writer.println("\t'A' Abort");
		String answer;
		boolean drop = false;
		do {
			answer = console.readLine();
			switch (answer.toUpperCase()) {
				case "Y":
					writer.println("Continuing and recreating schema");
					drop = true;
					break;
				case "N":
					writer.println("Continuing without dropping schema");
					drop = false;
					break;
				case "A":
					writer.println("Exiting");
					System.exit(0);
					break;
				default:
					writer.println("Please answer 'Y', 'N' or 'A'");
			}
		} while (!"Y".equalsIgnoreCase(answer) && !"N".equalsIgnoreCase(answer) && !"A".equalsIgnoreCase(answer));
		return drop;
	}
}
