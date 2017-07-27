package uk.gov.ons.sbr.data.hbase.util;

import org.junit.Test;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.time.YearMonth;

import static org.junit.Assert.*;

public class RowKeyUtilsTest {

    private static final YearMonth TEST_REFERENCE_PERIOD = YearMonth.of(2017, 07);
    private static final String TEST_ENTERPRISE_KEY = "123456789";
    private static final String[] TEST_COMPOSITE_KEY_PARTS = {"201707", TEST_ENTERPRISE_KEY};
    private static final String TEST_ROWKEY = String.join(RowKeyUtils.getDELIMETER(), TEST_COMPOSITE_KEY_PARTS);

    @Test
    public void splitRowKey() throws Exception {
        String[] compositeKeyParts = RowKeyUtils.splitRowKey(TEST_ROWKEY);
        assertEquals("Failure - invalid number of key components", TEST_COMPOSITE_KEY_PARTS.length, compositeKeyParts.length);
        for (int i = 0; i < TEST_COMPOSITE_KEY_PARTS.length; i++) {
            assertEquals("Failure - composite key part not the same", TEST_COMPOSITE_KEY_PARTS[i], compositeKeyParts[i]);
        }
    }

    @Test
    public void createRowKey() throws Exception {
        // Test generate row keys from Strings
        String rowKey = RowKeyUtils.createRowKey(TEST_COMPOSITE_KEY_PARTS);
        assertEquals("Failure - row key not the same", TEST_ROWKEY, rowKey);

        // Test generate row key form period + Strings
        rowKey = RowKeyUtils.createRowKey(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_KEY);
        assertEquals("Failure - row key not the same", TEST_ROWKEY, rowKey);
    }

    @Test
    public void  createEnterpriseFromRowKey() throws Exception {
        Enterprise enterprise = RowKeyUtils.createEnterpriseFromRowKey(TEST_ROWKEY);
        assertEquals("Failure - key not the same", TEST_ENTERPRISE_KEY, enterprise.getKey());
        assertEquals("Failure - reference period not the same", TEST_REFERENCE_PERIOD, enterprise.getReferencePeriod());
    }

}