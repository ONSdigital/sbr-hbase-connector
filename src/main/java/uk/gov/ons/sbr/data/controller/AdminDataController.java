package uk.gov.ons.sbr.data.controller;

import uk.gov.ons.sbr.data.dao.AdminDataDAO;
import uk.gov.ons.sbr.data.domain.CompanyRegistration;
import uk.gov.ons.sbr.data.hbase.dao.HBaseAdminDataDAO;
import uk.gov.ons.sbr.data.hbase.util.ReferencePeriodUtils;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Optional;

/**
 * Provides read and update access to the SBR Enterprise operational data
 */
public class AdminDataController {

    private AdminDataDAO adminDataDAO;

    public AdminDataController() {
        this.adminDataDAO = new HBaseAdminDataDAO();
    }

    public Optional<CompanyRegistration> getCompanyRegistration(String enterpriseReferenceNumber) throws IOException {
        return getCompanyRegistrationForReferencePeriod(ReferencePeriodUtils.getCurrentPeriod(), enterpriseReferenceNumber);
    }

    public Optional<CompanyRegistration> getCompanyRegistrationForReferencePeriod(YearMonth referencePeriod, String companyRegistrationNumber) throws IOException {
        return adminDataDAO.getCompanyRegistration(referencePeriod, companyRegistrationNumber);
    }

}
