package uk.gov.ons.sbr.data.hbase.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;

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

