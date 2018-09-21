package uk.gov.ons.sbr.data.hbase.util;

import java.time.Month;
import java.time.YearMonth;

public class ReferencePeriodUtils {

    private static final YearMonth HARDCODED_CURRENT_PERIOD = YearMonth.of(2017, Month.JUNE);

    public static  YearMonth getCurrentPeriod() {
        //TODO: Determine last loaded data period (and cache result)
        return HARDCODED_CURRENT_PERIOD;
    }

}
