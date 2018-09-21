package uk.gov.ons.sbr.data.dao;

import uk.gov.ons.sbr.data.domain.StatisticalUnit;
import uk.gov.ons.sbr.data.domain.StatisticalUnitLinks;
import uk.gov.ons.sbr.data.domain.UnitType;

import java.time.YearMonth;
import java.util.List;
import java.util.Optional;

public interface StatisticalUnitLinksDAO {

    Optional<List<StatisticalUnit>> scanUnits(YearMonth referencePeriod, String key) throws Exception;

    Optional<StatisticalUnitLinks> getUnitLinks(YearMonth referencePeriod, String key, UnitType type) throws Exception;

    void putUnitLinks(StatisticalUnitLinks links, UnitType type) throws Exception;
}
