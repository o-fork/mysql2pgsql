package com.adam.mysql2pgsql;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class PSQLExecutor {

	final String pgsqlHost;
	final String pgsqlDb;
	final Integer pgsqlPort;
	final String pgsqlUser;
	final String pgsqlPassword;

	public PSQLExecutor(String pgsqlHost, String pgsqlDb, Integer pgsqlPort, String pgsqlUser, String pgsqlPassword) {
		this.pgsqlHost = pgsqlHost;
		this.pgsqlDb = pgsqlDb;
		this.pgsqlPort = pgsqlPort;
		this.pgsqlUser = pgsqlUser;
		this.pgsqlPassword = pgsqlPassword;
	}

	@SuppressWarnings("SleepWhileInLoop")
	void executeFile(File file) throws IOException {
		System.out.println("ExecuteCommand called with file: " + file.getAbsolutePath());
		List<String> args = new LinkedList<>();
		args.add("psql");
		args.add("--dbname=" + pgsqlDb);
		args.add("-h" + pgsqlHost);
		args.add("--port=" + String.valueOf(pgsqlPort));
		args.add("-U" + pgsqlUser);
		args.add("--quiet");
		args.add("--file=" + file.getAbsolutePath());
		ProcessBuilder pb = new ProcessBuilder(args);
		pb.environment().put("PGPASSWORD", pgsqlPassword);
		pb.redirectErrorStream(true);
		Process process = pb.start();
		InputStream is = process.getInputStream();
		final BufferedReader br = new BufferedReader(new InputStreamReader(is));
		List<String> lines = new ArrayList<>();
		String line;
		while ((line = br.readLine()) != null) {
			if (line.isEmpty() || "\n".equals(line) || line.contains(" NOTICE:")) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException ex) {
				}
				continue;
			}
			System.out.println("PSQL: " + line);
			lines.add(line);
		}
		try {
			int exitCode = process.waitFor();
			if (exitCode != 0) {
				System.err.println("Got exit code " + exitCode + " from subprocess");
				throw new IOException(lines.toString());
			}
		} catch (InterruptedException ex) {
			throw new IOException(ex);
		}
	}
}
