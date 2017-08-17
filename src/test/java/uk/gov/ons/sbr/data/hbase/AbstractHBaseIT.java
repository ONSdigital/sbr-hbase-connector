package uk.gov.ons.sbr.data.hbase;

import org.junit.BeforeClass;

import static uk.gov.ons.sbr.data.hbase.HBaseConnector.IN_MEMORY_HBASE;

public abstract class AbstractHBaseIT {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(IN_MEMORY_HBASE, Boolean.TRUE.toString());
    }

}
