package uk.gov.ons.sbr.data.hbase.util;

import org.junit.Test;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.domain.StatisticalUnit;
import uk.gov.ons.sbr.data.domain.UnitType;

import java.time.YearMonth;

import static org.junit.Assert.assertEquals;

public class RowKeyUtilsTest {

    private static final YearMonth TEST_REFERENCE_PERIOD = YearMonth.of(2017, 07);
    private static final String TEST_KEY = "123456789";
    private static final String[] TEST_PERIOD_KEY_COMPOSITE_KEY_PARTS = {"201707", TEST_KEY};
    private static final String[] TEST_PERIOD_KEY_UNITTYPE_COMPOSITE_KEY_PARTS = {"201707", TEST_KEY, UnitType.LEGAL_UNIT.toString()};
    private static final String TEST_ENTERPRISE_ROWKEY = String.join(RowKeyUtils.getDELIMETER(), TEST_PERIOD_KEY_COMPOSITE_KEY_PARTS);
    private static final String TEST_UNIT_ROWKEY = String.join(RowKeyUtils.getDELIMETER(), TEST_PERIOD_KEY_UNITTYPE_COMPOSITE_KEY_PARTS);

    @Test
    public void splitRowKey() throws Exception {
        String[] compositeKeyParts = RowKeyUtils.splitRowKey(TEST_ENTERPRISE_ROWKEY);
        assertEquals("Failure - invalid number of key components", TEST_PERIOD_KEY_COMPOSITE_KEY_PARTS.length, compositeKeyParts.length);
        for (int i = 0; i < TEST_PERIOD_KEY_COMPOSITE_KEY_PARTS.length; i++) {
            assertEquals("Failure - composite key part not the same", TEST_PERIOD_KEY_COMPOSITE_KEY_PARTS[i], compositeKeyParts[i]);
        }
    }

    @Test
    public void createRowKey() throws Exception {
        // Test generate row keys from Strings
        String rowKey = RowKeyUtils.createRowKey(TEST_PERIOD_KEY_COMPOSITE_KEY_PARTS);
        assertEquals("Failure - row key not the same", TEST_ENTERPRISE_ROWKEY, rowKey);

        // Test generate row key form period + Strings
        rowKey = RowKeyUtils.createRowKey(TEST_REFERENCE_PERIOD, TEST_KEY);
        assertEquals("Failure - row key not the same", TEST_ENTERPRISE_ROWKEY, rowKey);
    }

    @Test
    public void  createUnitFromRowKey() throws Exception {
        Enterprise enterprise = RowKeyUtils.createUnitFromRowKey(TEST_ENTERPRISE_ROWKEY, UnitType.ENTERPRISE);
        assertEquals("Failure - key not the same", TEST_KEY, enterprise.getKey());
        assertEquals("Failure - reference period not the same", TEST_REFERENCE_PERIOD, enterprise.getReferencePeriod());
    }

    @Test
    public void createUnitOfUnknownTypeFromRowKey() throws Exception {
        StatisticalUnit statisticalUnit = RowKeyUtils.createUnitOfUnknownTypeFromRowKey(TEST_UNIT_ROWKEY);
        assertEquals("Failure - key not the same", TEST_KEY, statisticalUnit.getKey());
        assertEquals("Failure - reference period not the same", TEST_REFERENCE_PERIOD, statisticalUnit.getReferencePeriod());
        assertEquals("Failure - unit type not the same", UnitType.LEGAL_UNIT, statisticalUnit.getType());
    }
}