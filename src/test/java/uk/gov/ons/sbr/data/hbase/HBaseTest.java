package uk.gov.ons.sbr.data.hbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

public class HBaseTest {

    private static HBaseTestingUtility hBaseTestingUtility;

    public static void init() throws Exception {
        if (hBaseTestingUtility == null) {
            hBaseTestingUtility = new HBaseTestingUtility();
            hBaseTestingUtility.startMiniCluster();
            HBaseConnector.getInstance().setConfiguration(hBaseTestingUtility.getConfiguration());
            hBaseTestingUtility.createTable(TableNames.ENTERPRISE.getTableName(), ColumnFamilies.ENTERPRISE_DATA.getColumnFamily());
            hBaseTestingUtility.createTable(TableNames.UNIT_LINKS.getTableName(), ColumnFamilies.UNIT_LINKS_DATA.getColumnFamily());
        }
    }

    @Override
    protected void finalize() throws Throwable {
        hBaseTestingUtility.shutdownMiniCluster();
    }
}
