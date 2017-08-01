package uk.gov.ons.sbr.data.hbase.util;

import org.junit.Test;

import java.time.Month;

import static org.junit.Assert.*;

public class ReferencePeriodUtilsTest {

    @Test
    public void getCurrentPeriod() throws Exception {
        assertEquals("Failure - invalid year", 2017, ReferencePeriodUtils.getCurrentPeriod().getYear());
        assertEquals("Failure - invalid year", Month.JUNE, ReferencePeriodUtils.getCurrentPeriod().getMonth());
    }
}