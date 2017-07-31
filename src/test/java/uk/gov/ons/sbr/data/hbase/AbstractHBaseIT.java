package uk.gov.ons.sbr.data.hbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import uk.gov.ons.sbr.data.hbase.dao.HBaseEnterpriseDAO;
import uk.gov.ons.sbr.data.hbase.dao.HBaseUnitDAO;

import java.io.IOException;

public abstract class AbstractHBaseIT {

    private static HBaseTestingUtility hBaseTestingUtility;

    @BeforeClass
    public static void init() throws Exception {
        hBaseTestingUtility = new HBaseTestingUtility();
        hBaseTestingUtility.startMiniCluster();
        getHBaseTestingUtility().createTable(Bytes.toBytes(HBaseEnterpriseDAO.ENTERPRISE_TABLE_NAME), HBaseEnterpriseDAO.ENTERPRISE_CF);
        getHBaseTestingUtility().createTable(Bytes.toBytes(HBaseUnitDAO.UNIT_LINKS_TABLE_NAME), HBaseUnitDAO.UNIT_LINKS_CF);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        hBaseTestingUtility.shutdownMiniCluster();
    }

    protected static HBaseTestingUtility getHBaseTestingUtility() {
        return hBaseTestingUtility;
    }
}
