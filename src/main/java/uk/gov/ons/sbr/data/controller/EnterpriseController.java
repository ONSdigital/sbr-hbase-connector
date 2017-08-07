package uk.gov.ons.sbr.data.controller;

import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.dao.UnitDAO;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.domain.UnitLinks;
import uk.gov.ons.sbr.data.domain.UnitType;
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
public class EnterpriseController {

    private EnterpriseDAO enterpriseDAO;
    private UnitDAO unitLinksDAO;

    public EnterpriseController() {
        this.enterpriseDAO = new HBaseEnterpriseDAO();
        this.unitLinksDAO = new HBaseUnitDAO();
    }

    public Optional<Enterprise> getEnterprise(String enterpriseReferenceNumber) throws IOException {
        return getEnterprise(ReferencePeriodUtils.getCurrentPeriod(), enterpriseReferenceNumber);
    }

    public Optional<Enterprise> getEnterprise(YearMonth referencePeriod, String enterpriseReferenceNumber) throws IOException {
        Optional<Enterprise> enterprise = enterpriseDAO.getEnterprise(referencePeriod, enterpriseReferenceNumber);
        if (enterprise.isPresent()) {
            Optional<UnitLinks> links = unitLinksDAO.getUnitLinks(referencePeriod, enterpriseReferenceNumber, UnitType.ENTERPRISE);
            if (links.isPresent()) {
                enterprise.get().setLinks(unitLinksDAO.getUnitLinks(referencePeriod, enterpriseReferenceNumber, UnitType.ENTERPRISE).get());
            }
        }
        return enterprise;
    }

    public void updateEnterpriseVariableValue(YearMonth referencePeriod, String enterpriseReferenceNumber, String variableName, String newValue) throws IOException {
        Enterprise updatedEnterprise = new Enterprise(referencePeriod, enterpriseReferenceNumber);
        updatedEnterprise.putVariable(variableName, newValue);
        enterpriseDAO.putEnterprise(updatedEnterprise);
    }

    public void updateEnterpriseVariableValues(YearMonth referencePeriod, String enterpriseReferenceNumber, Map<String, String> newVariableValues) throws IOException {
        Enterprise updatedEnterprise = new Enterprise(referencePeriod, enterpriseReferenceNumber);
        updatedEnterprise.putVariables(newVariableValues);
        enterpriseDAO.putEnterprise(updatedEnterprise);
    }
}
