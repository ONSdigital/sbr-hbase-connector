package uk.gov.ons.sbr.data.hbase.table;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TableNamesTest {

    @Test
    public void getTableName() throws Exception {
        assertEquals("Failure = invalid table name", "sbr_local_db:enterprise", TableNames.ENTERPRISE.getTableName().getNameAsString());
        assertEquals("Failure = invalid table name", "sbr_local_db:unit_links", TableNames.UNIT_LINKS.getTableName().getNameAsString());
    }

}