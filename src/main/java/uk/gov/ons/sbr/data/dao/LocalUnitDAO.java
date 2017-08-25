package uk.gov.ons.sbr.data.dao;

import uk.gov.ons.sbr.data.domain.LocalUnit;

import java.time.YearMonth;
import java.util.Optional;

/**
 * Definition of an Local Unit DAO
 */
public interface LocalUnitDAO {

    Optional<LocalUnit> getLocalUnit(YearMonth referencePeriod, String key) throws Exception;

    void putLocalUnit(LocalUnit localUnit) throws Exception;
}
