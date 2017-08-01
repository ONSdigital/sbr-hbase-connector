package uk.gov.ons.sbr.data.hbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import uk.gov.ons.sbr.data.hbase.dao.HBaseEnterpriseDAO;
import uk.gov.ons.sbr.data.hbase.dao.HBaseUnitDAO;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

public abstract class AbstractHBaseIT {

    private static HBaseTestingUtility hBaseTestingUtility;

    @BeforeClass
    public static void init() throws Exception {
        hBaseTestingUtility = new HBaseTestingUtility();
        hBaseTestingUtility.startMiniCluster();
        getHBaseTestingUtility().createTable(TableNames.ENTERPRISE.getTableName(), ColumnFamilies.ENTERPRISE_DATA.getColumnFamily());
        getHBaseTestingUtility().createTable(TableNames.UNIT_LINKS.getTableName(), ColumnFamilies.UNIT_LINKS_DATA.getColumnFamily());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        hBaseTestingUtility.shutdownMiniCluster();
    }

    protected static HBaseTestingUtility getHBaseTestingUtility() {
        return hBaseTestingUtility;
    }
}
