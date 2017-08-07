package uk.gov.ons.sbr.data.hbase.table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Enumeration of table names in the SBR schema.
 */
public enum TableNames {

    ENTERPRISE("enterprise"),
    UNIT_LINKS("unit_links");

    private TableName tableName;

    TableNames(String tableName) {
        this.tableName = TableName.valueOf(System.getProperty("SBR_NAMESPACE", "sbr_local_db"), tableName);
    }

    public TableName getTableName() {
        return tableName;
    }
}
