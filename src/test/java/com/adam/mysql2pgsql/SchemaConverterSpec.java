package com.adam.mysql2pgsql;

import java.text.ParseException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author adam
 */
public class SchemaConverterSpec {

	@Test
	public void itShouldParseAutoIncrement() throws ParseException {
		SchemaConverter converter = new SchemaConverter("dummy", null, Integer.MIN_VALUE, null, null, null, null);
		TableMetaData tableMetaData = new TableMetaData("dummy");
		converter.convertAutoIncrementInstruction(tableMetaData, "`id` int(10) unsigned NOT NULL AUTO_INCREMENT");
		Assert.assertEquals("CREATE TABLE \"schema\".\"dummy\" (\n\t\"id\" serial\n);", tableMetaData.generateCreateTableStatement("schema"));
	}
}
