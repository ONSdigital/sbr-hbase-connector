package uk.gov.ons.sbr.data.hbase.dao;

import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.domain.UnitType;

import java.time.YearMonth;
import java.util.Optional;

/**
 * HBase implementation of an Enterprise DAO
 */
public class HBaseEnterpriseDAO extends HBaseStatisticalUnitDAO implements EnterpriseDAO {

    @Override
    public Optional<Enterprise> getEnterprise(YearMonth referencePeriod, String key) throws Exception {
        return getUnit(UnitType.ENTERPRISE, referencePeriod, key);
    }

    @Override
    public void putEnterprise(Enterprise enterprise, String updatedBy) throws Exception {
        putUnit(enterprise, updatedBy);
    }

}
