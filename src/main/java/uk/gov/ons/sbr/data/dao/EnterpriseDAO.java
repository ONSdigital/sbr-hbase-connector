package uk.gov.ons.sbr.data.dao;

import uk.gov.ons.sbr.data.domain.Enterprise;

import java.time.YearMonth;
import java.util.Optional;

/**
 * Definition of an Enterprise DAO
 */
public interface EnterpriseDAO {

    Optional<Enterprise> getEnterprise(YearMonth referencePeriod, String key) throws Exception;

    void putEnterprise(Enterprise enterprise) throws Exception;
}
