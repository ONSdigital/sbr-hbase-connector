package uk.gov.ons.sbr.data.controller;

import uk.gov.ons.sbr.data.dao.CompanyRegistrationDAO;
import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.dao.UnitDAO;
import uk.gov.ons.sbr.data.domain.CompanyRegistration;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.domain.UnitLinks;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.dao.HBaseCompanyRegistrationDAO;
import uk.gov.ons.sbr.data.hbase.dao.HBaseEnterpriseDAO;
import uk.gov.ons.sbr.data.hbase.dao.HBaseUnitDAO;
import uk.gov.ons.sbr.data.hbase.util.ReferencePeriodUtils;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Map;
import java.util.Optional;

/**
 * Provides read and update access to the SBR Enterprise operational data
 */
public class CompanyRegistrationController {

    private CompanyRegistrationDAO companyRegistrationDAO;

    public CompanyRegistrationController() {
        this.companyRegistrationDAO = new HBaseCompanyRegistrationDAO();
    }

    public Optional<CompanyRegistration> getCompanyRegistration(String enterpriseReferenceNumber) throws IOException {
        return getCompanyRegistrationForReferencePeriod(ReferencePeriodUtils.getCurrentPeriod(), enterpriseReferenceNumber);
    }

    public Optional<CompanyRegistration> getCompanyRegistrationForReferencePeriod(YearMonth referencePeriod, String companyRegistrationNumber) throws IOException {
        return companyRegistrationDAO.getCompanyRegistration(referencePeriod, companyRegistrationNumber);
    }

}
