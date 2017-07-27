package uk.gov.ons.sbr.data.hbase.dao;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.ons.sbr.data.hbase.HBaseConfig;

import java.io.IOException;


public class HBaseEnterpriseDAOIT extends AbstractHBaseEnterpriseDAOTest {

    private static HBaseTestingUtility utility;

    @BeforeClass
    public static void init() throws Exception {
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
        utility.createTable(Bytes.toBytes(HBaseEnterpriseDAO.ENTERPRISE_TABLE_NAME), HBaseEnterpriseDAO.ENTERPRISE_CF);
        utility.getConfiguration();
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        setConfig(new HBaseConfig(utility.getConfiguration()));
        setDao(new HBaseEnterpriseDAO(getConfig()));
    }

    @Test
    public void getEnterprise() throws Exception {
        super.putEnterprise();
        super.getEnterprise();
    }


    @Test(expected = IOException.class)
    public void getEnterpriseFailedConnection() throws Exception {
        //TODO: Force a IOException from HBase
        throw new IOException();
    }

    @Test
    public void putEnterprise() throws Exception {
        super.putEnterprise();
        super.getEnterprise();
    }

    @Test(expected = IOException.class)
    public void putEnterpriseFailedConnection() throws Exception {
        //TODO: Force a IOException from HBase
        throw new IOException();
    }

}