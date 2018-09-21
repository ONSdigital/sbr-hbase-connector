package uk.gov.ons.sbr.data.controller;

import uk.gov.ons.sbr.data.dao.AdminDataDAO;
import uk.gov.ons.sbr.data.domain.CompanyRegistration;
import uk.gov.ons.sbr.data.domain.PAYEReturn;
import uk.gov.ons.sbr.data.domain.VATReturn;
import uk.gov.ons.sbr.data.hbase.dao.HBaseAdminDataDAO;
import uk.gov.ons.sbr.data.hbase.util.ReferencePeriodUtils;

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

    public Optional<CompanyRegistration> getCompanyRegistration(String enterpriseReferenceNumber) throws Exception {
        return getCompanyRegistrationForReferencePeriod(ReferencePeriodUtils.getCurrentPeriod(), enterpriseReferenceNumber);
    }

    public Optional<CompanyRegistration> getCompanyRegistrationForReferencePeriod(YearMonth referencePeriod, String companyRegistrationNumber) throws Exception {
        return adminDataDAO.getCompanyRegistration(referencePeriod, companyRegistrationNumber);
    }

    public Optional<PAYEReturn> getPAYEReturn(String payeReferenceNumber) throws Exception {
        return getPAYEReturnForReferencePeriod(ReferencePeriodUtils.getCurrentPeriod(), payeReferenceNumber);
    }

    public Optional<PAYEReturn> getPAYEReturnForReferencePeriod(YearMonth referencePeriod, String payeReferenceNumber) throws Exception {
        return adminDataDAO.getPAYEReturn(referencePeriod, payeReferenceNumber);
    }

    public Optional<VATReturn> getVATReturn(String vatReferenceNumber) throws Exception {
        return getVATReturnForReferencePeriod(ReferencePeriodUtils.getCurrentPeriod(), vatReferenceNumber);
    }

    public Optional<VATReturn> getVATReturnForReferencePeriod(YearMonth referencePeriod, String vatReferenceNumber) throws Exception {
        return adminDataDAO.getVATReturn(referencePeriod, vatReferenceNumber);
    }

}
