package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;

/**
 * Represents a PAYE Unit
 */
public class PAYEReturn extends StatisticalUnit {

    public PAYEReturn(YearMonth referencePeriod, String key) {
        super(referencePeriod, key, UnitType.PAYE);
    }

}
