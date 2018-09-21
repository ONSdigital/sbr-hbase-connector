package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;

/**
 * Represents an Enterprise Unit
 */
public class Enterprise extends StatisticalUnit {

    public Enterprise(YearMonth referencePeriod, String key) {
        super(referencePeriod, key, UnitType.ENTERPRISE);
    }

}
