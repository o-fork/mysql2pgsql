package com.adam.mysql2pgsql;

import java.util.ArrayList;
import java.util.List;

public class TableMetaData {

	final String tableName;
	final List<String> colDefinitions;
	final List<String> comments;
	final List<String> constraints;
	final List<String> pks;
	final List<String> indices;
	final List<String> postSqls;

	TableMetaData(String tableName) {
		this.tableName = tableName;
		this.colDefinitions = new ArrayList<>();
		this.comments = new ArrayList<>();
		this.constraints = new ArrayList<>();
		this.pks = new ArrayList<>();
		this.indices = new ArrayList<>();
		this.postSqls = new ArrayList<>();
	}

	String getTableName() {
		return tableName;
	}

	void addColDefinition(String colDefinition) {
		this.colDefinitions.add(colDefinition);
	}

	void addComment(String comment) {
		this.comments.add(comment);
	}

	void addPostSQL(String postSql) {
		this.postSqls.add(postSql);
	}

	void addConstraint(String constraint) {
		this.constraints.add(constraint);
	}

	void addPk(String pk) {
		this.pks.add(pk);
	}

	void addIndex(String index) {
		this.indices.add(index);
	}

	String generateCreateTableStatement(String schemaName) {
		String retStr = "";
		retStr += String.format("DROP TABLE IF EXISTS \"%s\".\"%s\" CASCADE;\n", schemaName, tableName);
		retStr += String.format("CREATE TABLE \"%s\".\"%s\" (\n", schemaName, tableName);
		for (int i = 0; i < colDefinitions.size(); i++) {
			if (i < colDefinitions.size() - 1) {
				retStr += "\t" + colDefinitions.get(i) + ",\n";
			} else {
				retStr += "\t" + colDefinitions.get(i) + "\n";
			}
		}
		retStr += ");";
		if (!comments.isEmpty()) {
			retStr += "\n";
			for (String comment : comments) {
				retStr += comment + "\n";
			}
		}
		return retStr;
	}

	String generateIndicesStatement() {
		if (indices.isEmpty()) {
			return null;
		}
		String retStr = "";
		for (int i = 0; i < indices.size(); i++) {
			retStr += indices.get(i) + ";\n";
		}
		return retStr;
	}

	String generateConstraintsStatement() {
		if (constraints.isEmpty()) {
			return null;
		}
		String retStr = "";
		for (int i = 0; i < constraints.size(); i++) {
			retStr += constraints.get(i) + ";\n";
		}
		return retStr;
	}

	String generatePkStatement() {
		if (pks.isEmpty()) {
			return null;
		}
		String retStr = "";
		for (int i = 0; i < pks.size(); i++) {
			retStr += pks.get(i) + ";\n";
		}
		return retStr;
	}

	String generatePostSqls() {
		if (postSqls.isEmpty()) {
			return null;
		}
		String retStr = "";
		for (String postSql : postSqls) {
			retStr += postSql + ";\n";
		}
		return retStr;
	}

	@Override
	public String toString() {
		return "TableMetaData{" + "\n  tableName=" + tableName + "\n, colDefinitions=" + colDefinitions + "\n, comments=" + comments +
				"\n, constraints=" + constraints + "\n, pks=" + pks + "\n, indices=" + indices + "\n, postSqls=" + postSqls + '}';
	}
}
