package uk.gov.ons.sbr.data.hbase.table;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import uk.gov.ons.sbr.data.domain.UnitType;

/**
 * Enumeration of table names in the SBR schema.
 */
public enum TableNames {

    COMPANIES_HOUSE_DATA("ch"),
    ENTERPRISE("enterprise"),
    PAYE("paye"),
    UNIT_LINKS("unit_links"),
    VAT("vat");

    private TableName tableName;

    TableNames(String tableName) {
        this.tableName = TableName.valueOf(System.getProperty("sbr.hbase.namespace", "sbr_local_db"), tableName);
    }

    public TableName getTableName() {
        return tableName;
    }

    public static TableName forUnitType(UnitType type){
        switch (type) {
            case COMPANY_REGISTRATION:
                return COMPANIES_HOUSE_DATA.getTableName();
            case ENTERPRISE:
                return ENTERPRISE.getTableName();
            case PAYE:
                return PAYE.getTableName();
            case VAT:
                return VAT.getTableName();
            default:
                return null;
        }
    }

}
