package uk.gov.ons.sbr.data.hbase.util;

import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.domain.Unit;
import uk.gov.ons.sbr.data.domain.UnitType;

import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

public class RowKeyUtils {

    private static final String REFERENCE_PERIOD_FORMAT = "yyyyMM";
    private static final String DELIMETER = "~";

    public static String getDELIMETER() {
        return DELIMETER;
    }

    public static String getReferencePeriodFormat() {
        return REFERENCE_PERIOD_FORMAT;
    }

    public static String[] splitRowKey(String rowKey) {
        return rowKey.split(DELIMETER);
    }

    public static String createRowKey(YearMonth referencePeriod, String... rowKeyParts) {
        String prefix = referencePeriod.format(DateTimeFormatter.ofPattern(REFERENCE_PERIOD_FORMAT));
        String suffix = createRowKey(rowKeyParts);
        String rowKey;
        if (suffix.isEmpty()) {
            rowKey = prefix;
        } else {
            rowKey = prefix + DELIMETER + suffix;
        }
        return rowKey;
    }

    public static String createRowKey(String... rowKeyParts) {
        return String.join(DELIMETER, rowKeyParts);
    }

    public static Enterprise createEnterpriseFromRowKey(String rowKey) {
        final String[] compositeRowKeyParts = RowKeyUtils.splitRowKey(rowKey);
        final YearMonth referencePeriod = YearMonth.parse(compositeRowKeyParts[0], DateTimeFormatter.ofPattern(REFERENCE_PERIOD_FORMAT));
        final String key = compositeRowKeyParts[1];
        return new Enterprise(referencePeriod, key);
    }

    public static Unit createUnitFromRowKey(String rowKey) {

        final String[] compositeRowKeyParts = RowKeyUtils.splitRowKey(rowKey);
        final YearMonth referencePeriod = YearMonth.parse(compositeRowKeyParts[0], DateTimeFormatter.ofPattern(REFERENCE_PERIOD_FORMAT));
        final String key = compositeRowKeyParts[1];
        final UnitType type = UnitType.fromString(compositeRowKeyParts[2]);
        return new Unit(referencePeriod, key, type);
    }
}
