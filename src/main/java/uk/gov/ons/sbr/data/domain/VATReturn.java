package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;

/**
 * Represents a VAT Unit
 */
public class VATReturn extends StatisticalUnit {

    public VATReturn(YearMonth referencePeriod, String key) {
        super(referencePeriod, key, UnitType.VAT);
    }

}
