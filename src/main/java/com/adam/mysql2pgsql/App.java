package com.adam.mysql2pgsql;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;

public class App {

	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, ParseException {
		Console console = System.console();
		if (console == null) {
			System.err.println("Need a console for interactivity");
			System.exit(1);
			return;
		}
		PrintWriter writer = console.writer();
		if (args == null || args.length != 9) {
			writer.append("Missing arguments!\n");
			writer.append("Required arguments missing!\n");
			writer.append("Please invoce the script with the following arguments: \n");
			writer.append("\tmysqlhost mysqlport mysqluser mysqlschema pgsqlhost pgsqlport pgsqldb pgsqluser pgsqlschema\n");
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

		String pgsqlUrl = "jdbc:postgresql://" + pgsqlHost + ":" + pgsqlPort + "/";
		DriverManager.registerDriver((java.sql.Driver) Class.forName("org.postgresql.Driver").newInstance());

		SchemaConverter schemaConverter = new SchemaConverter(mysqlSchema, mysqlHost, mysqlPort, mysqlUser, mysqlPassword, pgsqlSchema);
		PSQLExecutor psqle = new PSQLExecutor(pgsqlHost, pgsqlDb, pgsqlPort, pgsqlUser, pgsqlPassword);

		//Parse mysql schema
		writer.println("Dumping and parsing mysql schema...");
		schemaConverter.generateMysqlDumpData();
		schemaConverter.parseSchemaDump();
		writer.println("Done\n");

		//Security check
		writer.println("Will now drop possibly existing schema \"" + pgsqlSchema + "\" and recreate it.");
		writer.println("Please verify that this is what's intended (Y/N)");
		String answer;
		do {
			answer = console.readLine();
			switch (answer.toUpperCase()) {
				case "N":
					System.out.println("Exiting");
					System.exit(0);
					break;
				case "Y":
					System.out.println("Continuing");
					break;
				default:
					System.out.println("Please answer 'Y' or 'N'");
			}
		} while (!"Y".equalsIgnoreCase(answer) && !"N".equalsIgnoreCase(answer));

		//Apply converted schema definition in postgres
		System.out.println("Deleting and creating schema in postgres...");
		File postgresTableDefFile = schemaConverter.generatePostgresTableDefinitionFile(pgsqlUser);
		System.out.println(postgresTableDefFile.getAbsolutePath());
		psqle.executeFile(postgresTableDefFile);
		System.out.println("Done\n");

		//Migrate all data to the new schema
		System.out.println("Migrating actual data from mysql to posgres...");
		try (DataMigrator dataMigrator = new DataMigrator(mysqlUrl, mysqlUser, mysqlPassword, mysqlSchema, pgsqlUrl, pgsqlUser, pgsqlPassword, pgsqlSchema)) {
			dataMigrator.transferTables();
		} catch (SQLException sqle) {
			sqle.printStackTrace(System.out);
			SQLException nextException = sqle.getNextException();
			if (nextException != null) {
				sqle.getNextException().printStackTrace(System.out);
			}
			throw sqle;
		}
		System.out.println("Done\n");

		//Apply all constraints and indices
		System.out.println("Applying pk constraints...");
		File postgresPkDefFile = schemaConverter.generatePostgresPkDefFile();
		psqle.executeFile(postgresPkDefFile);
		System.out.println("Done.\n");

		System.out.println("Applying fk constraints and creating indices...");
		File postgresIdxAndConstraintsFile = schemaConverter.generatePostgresIndexAndConstraintsFile();
		System.out.println(postgresIdxAndConstraintsFile.getAbsolutePath());
		psqle.executeFile(postgresIdxAndConstraintsFile);
		System.out.println("Done\n");

		System.out.println("Running post SQLs: Updaing sequences to current increment value...");
		File postSqlFile = schemaConverter.generatePostSqlFile();
		System.out.println(postSqlFile.getAbsolutePath());
		psqle.executeFile(postSqlFile);
		System.out.println("Done\n");

		//All done
		System.out.println("All done");

	}
}
