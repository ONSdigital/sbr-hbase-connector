package uk.gov.ons.sbr.data.hbase.dao;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.ons.sbr.data.hbase.HBaseTest;

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