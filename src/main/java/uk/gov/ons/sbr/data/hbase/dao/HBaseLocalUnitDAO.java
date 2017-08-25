package uk.gov.ons.sbr.data.hbase.dao;

import uk.gov.ons.sbr.data.dao.LocalUnitDAO;
import uk.gov.ons.sbr.data.domain.LocalUnit;
import uk.gov.ons.sbr.data.domain.UnitType;

import java.time.YearMonth;
import java.util.Optional;

/**
 * HBase implementation of an Enterprise DAO
 */
public class HBaseLocalUnitDAO extends HBaseStatisticalUnitDAO implements LocalUnitDAO {

    @Override
    public Optional<LocalUnit> getLocalUnit(YearMonth referencePeriod, String key) throws Exception {
        return getUnit(UnitType.LOCAL_UNIT, referencePeriod, key);
    }

    @Override
    public void putLocalUnit(LocalUnit localUnit) throws Exception {
        putUnit(localUnit);
    }

}

