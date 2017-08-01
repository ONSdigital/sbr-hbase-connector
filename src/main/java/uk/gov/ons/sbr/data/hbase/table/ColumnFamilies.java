package uk.gov.ons.sbr.data.hbase.table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Enumeration of table column families in the SBR schema.
 */
public enum ColumnFamilies {

    ENTERPRISE_DATA("d"),
    UNIT_LINKS_DATA("d");

    private byte[] columnFamily;

    ColumnFamilies(String columnFamily) {
        this.columnFamily = Bytes.toBytes(columnFamily);
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }
}
