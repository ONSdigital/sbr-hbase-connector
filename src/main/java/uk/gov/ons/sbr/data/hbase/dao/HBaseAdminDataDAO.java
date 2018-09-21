package uk.gov.ons.sbr.data.hbase.dao;

import uk.gov.ons.sbr.data.dao.AdminDataDAO;
import uk.gov.ons.sbr.data.domain.CompanyRegistration;
import uk.gov.ons.sbr.data.domain.PAYEReturn;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.domain.VATReturn;

import java.time.YearMonth;
import java.util.Optional;

/**
 * HBase implementation of an Admin Data DAO
 */
public class HBaseAdminDataDAO extends HBaseStatisticalUnitDAO implements AdminDataDAO {

    @Override
    public Optional<CompanyRegistration> getCompanyRegistration(YearMonth referencePeriod, String key) throws Exception {
        return getUnit(UnitType.COMPANY_REGISTRATION, referencePeriod, key);
    }

    @Override
    public Optional<VATReturn> getVATReturn(YearMonth referencePeriod, String key) throws Exception {
        return getUnit(UnitType.VAT, referencePeriod, key);
    }

    @Override
    public Optional<PAYEReturn> getPAYEReturn(YearMonth referencePeriod, String key) throws Exception {
        return getUnit(UnitType.PAYE, referencePeriod, key);
    }

}

