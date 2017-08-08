package uk.gov.ons.sbr.data.dao;

import uk.gov.ons.sbr.data.domain.CompanyRegistration;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Optional;

/**
 * Definition of an Company Registration DAO
 */
public interface CompanyRegistrationDAO {

    Optional<CompanyRegistration> getCompanyRegistration(YearMonth referencePeriod, String key) throws IOException;

}
