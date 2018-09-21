package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;

/**
 * Represents an Company Registration Unit
 */
public class CompanyRegistration extends StatisticalUnit {

    public CompanyRegistration(YearMonth referencePeriod, String key) {
        super(referencePeriod, key, UnitType.COMPANY_REGISTRATION);
    }

}
