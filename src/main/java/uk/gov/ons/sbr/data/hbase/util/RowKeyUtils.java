package uk.gov.ons.sbr.data.hbase.util;

import uk.gov.ons.sbr.data.domain.StatisticalUnit;
import uk.gov.ons.sbr.data.domain.StatisticalUnitFactory;
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

    @SuppressWarnings("unchecked")
    public static <T extends StatisticalUnit> T createUnitFromRowKey(String rowKey, UnitType unitType) {
        final String[] compositeRowKeyParts = RowKeyUtils.splitRowKey(rowKey);
        final YearMonth referencePeriod = YearMonth.parse(compositeRowKeyParts[0], DateTimeFormatter.ofPattern(REFERENCE_PERIOD_FORMAT));
        final String key = compositeRowKeyParts[1];
        StatisticalUnit statisticalUnit = StatisticalUnitFactory.getUnit(unitType, referencePeriod, key);
        return (T)statisticalUnit;
    }

    public static StatisticalUnit createUnitOfUnknownTypeFromRowKey(String rowKey) {
        final String[] compositeRowKeyParts = RowKeyUtils.splitRowKey(rowKey);
        final UnitType type = UnitType.fromString(compositeRowKeyParts[2]);
        return createUnitFromRowKey(rowKey, type);
    }
}
