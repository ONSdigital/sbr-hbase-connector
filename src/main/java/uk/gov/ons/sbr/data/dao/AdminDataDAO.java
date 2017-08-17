package uk.gov.ons.sbr.data.dao;

import uk.gov.ons.sbr.data.domain.CompanyRegistration;
import uk.gov.ons.sbr.data.domain.PAYEReturn;
import uk.gov.ons.sbr.data.domain.VATReturn;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Optional;

/**
 * Definition of an Admin Data DAO
 */
public interface AdminDataDAO {

    Optional<CompanyRegistration> getCompanyRegistration(YearMonth referencePeriod, String key) throws Exception;

    Optional<VATReturn> getVATReturn(YearMonth referencePeriod, String key) throws Exception;

    Optional<PAYEReturn> getPAYEReturn(YearMonth referencePeriod, String key) throws Exception;

}
