package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;

/**
 * Represents an Local Unit
 */
public class LocalUnit extends StatisticalUnit {

    public LocalUnit(YearMonth referencePeriod, String key) {
        super(referencePeriod, key, UnitType.LOCAL_UNIT);
    }

}
