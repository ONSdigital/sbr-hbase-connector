package uk.gov.ons.sbr.data.hbase;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

/**
 * Create in memory test instance of the SBR Hbase schema
 */
public class HBaseTest {

    private static HBaseTestingUtility hBaseTestingUtility;

    public static void init() throws Exception {
        if (hBaseTestingUtility == null) {
            hBaseTestingUtility = new HBaseTestingUtility();
            hBaseTestingUtility.setJobWithoutMRCluster();
            hBaseTestingUtility.startMiniCluster();
            HBaseConnector.getInstance().setConfiguration(hBaseTestingUtility.getConfiguration());
            hBaseTestingUtility.createTable(TableNames.ENTERPRISE.getTableName(), ColumnFamilies.ENTERPRISE_DATA.getColumnFamily());
            hBaseTestingUtility.createTable(TableNames.UNIT_LINKS.getTableName(), ColumnFamilies.UNIT_LINKS_DATA.getColumnFamily());
            hBaseTestingUtility.createTable(TableNames.COMPANIES_HOUSE_DATA.getTableName(), ColumnFamilies.COMPANY_DATA.getColumnFamily());
            hBaseTestingUtility.createTable(TableNames.VAT.getTableName(), ColumnFamilies.VAT_DATA.getColumnFamily());
            hBaseTestingUtility.createTable(TableNames.PAYE.getTableName(), ColumnFamilies.PAYE_DATA.getColumnFamily());
        }
    }

    @Override
    protected void finalize() throws Throwable {
        hBaseTestingUtility.cleanupDataTestDirOnTestFS(TableNames.ENTERPRISE.getTableName().getNameAsString());
        hBaseTestingUtility.cleanupDataTestDirOnTestFS(TableNames.UNIT_LINKS.getTableName().getNameAsString());
        hBaseTestingUtility.cleanupDataTestDirOnTestFS(TableNames.COMPANIES_HOUSE_DATA.getTableName().getNameAsString());
        hBaseTestingUtility.shutdownMiniCluster();
    }
}
