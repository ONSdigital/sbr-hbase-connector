package uk.gov.ons.sbr.data.controller;

import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.hbase.HBaseConfig;
import uk.gov.ons.sbr.data.hbase.dao.HBaseEnterpriseDAO;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Map;
import java.util.Optional;

/**
 * Provides read and update access to the SBR Enterprise operational data
 */
public class EnterpriseController {


    private EnterpriseDAO enterpriseDAO;

    public EnterpriseController(HBaseConfig config) {
        this.enterpriseDAO = new HBaseEnterpriseDAO(config);
    }

    public Optional<Enterprise> getEnterprise(YearMonth referencePeriod, String enterpriseReferenceNumber) throws IOException {
        return enterpriseDAO.getEnterprise(referencePeriod, enterpriseReferenceNumber);
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
