package uk.gov.ons.sbr.data.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

/**
 * Create in memory test instance of the SBR Hbase schema
 */
public class InMemoryHBase {

    private static HBaseTestingUtility hBaseTestingUtility;

    public static Configuration init() throws Exception {
        if (hBaseTestingUtility == null) {
            hBaseTestingUtility = new HBaseTestingUtility();
            hBaseTestingUtility.setJobWithoutMRCluster();
            hBaseTestingUtility.startMiniCluster();
            hBaseTestingUtility.createTable(TableNames.ENTERPRISE.getTableName(), ColumnFamilies.ENTERPRISE_DATA.getColumnFamily());
            hBaseTestingUtility.createTable(TableNames.UNIT_LINKS.getTableName(), ColumnFamilies.UNIT_LINKS_DATA.getColumnFamily());
            hBaseTestingUtility.createTable(TableNames.COMPANIES_HOUSE_DATA.getTableName(), ColumnFamilies.COMPANY_DATA.getColumnFamily());
            hBaseTestingUtility.createTable(TableNames.VAT.getTableName(), ColumnFamilies.VAT_DATA.getColumnFamily());
            hBaseTestingUtility.createTable(TableNames.PAYE.getTableName(), ColumnFamilies.PAYE_DATA.getColumnFamily());
        }
        return hBaseTestingUtility.getConfiguration();
    }

    @Override
    protected void finalize() throws Throwable {
        hBaseTestingUtility.cleanupDataTestDirOnTestFS(TableNames.ENTERPRISE.getTableName().getNameAsString());
        hBaseTestingUtility.cleanupDataTestDirOnTestFS(TableNames.UNIT_LINKS.getTableName().getNameAsString());
        hBaseTestingUtility.cleanupDataTestDirOnTestFS(TableNames.COMPANIES_HOUSE_DATA.getTableName().getNameAsString());
        hBaseTestingUtility.cleanupDataTestDirOnTestFS(TableNames.VAT.getTableName().getNameAsString());
        hBaseTestingUtility.cleanupDataTestDirOnTestFS(TableNames.PAYE.getTableName().getNameAsString());
        hBaseTestingUtility.shutdownMiniCluster();
    }
}
