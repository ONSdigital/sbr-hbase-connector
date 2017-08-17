package uk.gov.ons.sbr.data.dao;

import uk.gov.ons.sbr.data.domain.StatisticalUnit;
import uk.gov.ons.sbr.data.domain.UnitLinks;
import uk.gov.ons.sbr.data.domain.UnitType;

import java.io.IOException;
import java.time.YearMonth;
import java.util.List;
import java.util.Optional;

public interface StatisticalUnitLinksDAO {

    Optional<List<StatisticalUnit>> scanUnits(YearMonth referencePeriod, String key) throws Exception;

    Optional<UnitLinks> getUnitLinks(YearMonth referencePeriod, String key, UnitType type) throws Exception;

    void putUnitLinks(UnitLinks links, UnitType type) throws Exception;
}
