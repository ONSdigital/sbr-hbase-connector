package uk.gov.ons.sbr.data.hbase.table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import uk.gov.ons.sbr.data.domain.UnitType;

/**
 * Enumeration of table column families in the SBR schema.
 */
public enum ColumnFamilies {

    COMPANY_DATA("d"),
    ENTERPRISE_DATA("d"),
    UNIT_LINKS_DATA("d");

    private byte[] columnFamily;

    ColumnFamilies(String columnFamily) {
        this.columnFamily = Bytes.toBytes(columnFamily);
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public static byte[] forUnitType(UnitType type){
        switch (type) {
            case COMPANY_REGISTRATION:
                return COMPANY_DATA.getColumnFamily();
            case ENTERPRISE:
                return ENTERPRISE_DATA.getColumnFamily();
            default:
                return null;
        }
    }
}
