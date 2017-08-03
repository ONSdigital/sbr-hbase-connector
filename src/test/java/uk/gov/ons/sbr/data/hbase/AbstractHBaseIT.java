package uk.gov.ons.sbr.data.hbase;

import org.junit.BeforeClass;

public abstract class AbstractHBaseIT {

    @BeforeClass
    public static void init() throws Exception {
        HBaseTest.init();
    }

}
