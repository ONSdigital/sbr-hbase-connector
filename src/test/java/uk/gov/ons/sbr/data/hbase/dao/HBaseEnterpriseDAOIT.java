package uk.gov.ons.sbr.data.hbase.dao;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.ons.sbr.data.hbase.HBaseConnector;
import uk.gov.ons.sbr.data.hbase.HBaseTest;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

public class HBaseEnterpriseDAOIT extends AbstractHBaseEnterpriseDAOTest {

    @BeforeClass
    public static void init() throws Exception {
        HBaseTest.init();
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        setDao(new HBaseEnterpriseDAO());
    }

    @Test
    public void getEnterprise() throws Exception {
        super.putEnterprise();
        super.getEnterprise();
    }

    @Test
    public void putEnterprise() throws Exception {
        super.putEnterprise();
        super.getEnterprise();
    }

}