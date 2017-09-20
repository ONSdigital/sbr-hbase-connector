package uk.gov.ons.sbr.data.controller;

import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.dao.StatisticalUnitLinksDAO;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.domain.StatisticalUnit;
import uk.gov.ons.sbr.data.domain.UnitLinks;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.dao.HBaseEnterpriseDAO;
import uk.gov.ons.sbr.data.hbase.dao.HBaseStatisticalUnitLinksDAO;
import uk.gov.ons.sbr.data.hbase.util.ReferencePeriodUtils;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides read and update access to the SBR Enterprise operational data
 */
public class EnterpriseController {

    private EnterpriseDAO enterpriseDAO;
    private StatisticalUnitLinksDAO unitLinksDAO;
    private ConcurrentHashMap<String, Optional<Enterprise>> cache = new ConcurrentHashMap<>();

    public EnterpriseController() {
        this.enterpriseDAO = new HBaseEnterpriseDAO();
        this.unitLinksDAO = new HBaseStatisticalUnitLinksDAO();
    }

    public Optional<Enterprise> getEnterprise(String enterpriseReferenceNumber) throws Exception {
        return getEnterpriseForReferencePeriod(ReferencePeriodUtils.getCurrentPeriod(), enterpriseReferenceNumber);
    }

    public Optional<Enterprise> getEnterpriseForReferencePeriod(YearMonth referencePeriod, String enterpriseReferenceNumber) throws Exception {
        String cacheKey = RowKeyUtils.createRowKey(referencePeriod, enterpriseReferenceNumber);
        if (cache.containsKey(cacheKey)) {
            return cache.get(cacheKey);
        } else {
            Optional<Enterprise> enterprise = enterpriseDAO.getEnterprise(referencePeriod, enterpriseReferenceNumber);
            if (enterprise.isPresent()) {
                Optional<UnitLinks> links = unitLinksDAO.getUnitLinks(referencePeriod, enterpriseReferenceNumber, UnitType.ENTERPRISE);
                if (links.isPresent()) {
                    enterprise.get().setLinks(links.get());
                    enterprise = Optional.of((Enterprise) createUnitHierachy(referencePeriod, enterprise.get(), links.get()));
                }
                cache.put(cacheKey, enterprise);
            }
            return enterprise;
        }
    }

    public void updateEnterpriseVariableValue(YearMonth referencePeriod, String enterpriseReferenceNumber, String updatedBy, String variableName, String newValue) throws Exception {
        Enterprise updatedEnterprise = new Enterprise(referencePeriod, enterpriseReferenceNumber);
        updatedEnterprise.putVariable(variableName, newValue);
        updateEnterprise(updatedEnterprise, updatedBy);
    }

    public void updateEnterpriseVariableValues(YearMonth referencePeriod, String enterpriseReferenceNumber, String updatedBy, Map<String, String> newVariableValues) throws Exception {
        Enterprise updatedEnterprise = new Enterprise(referencePeriod, enterpriseReferenceNumber);
        updatedEnterprise.putVariables(newVariableValues);
        updateEnterprise(updatedEnterprise, updatedBy);
    }

    private void updateEnterprise(Enterprise updatedEnterprise, String updatedBy) throws Exception {
        enterpriseDAO.putEnterprise(updatedEnterprise, updatedBy);
        String cacheKey = RowKeyUtils.createRowKey(updatedEnterprise.getReferencePeriod(), updatedEnterprise.getKey());
        cache.remove(cacheKey);
    }

    private StatisticalUnit createUnitHierachy(YearMonth referencePeriod, StatisticalUnit parentUnit, UnitLinks links) throws Exception {
        List<UnitType> directDescendants = parentUnit.getType().getDirectDescendants();
        Map<String, UnitType> children = links.getChildren();
        UnitType childType;
        for (String childKey : children.keySet()) {
            if (directDescendants.contains(children.get(childKey))) {
                childType = children.get(childKey);
                StatisticalUnit childUnit = new StatisticalUnit(referencePeriod, childKey, childType);
                Optional<UnitLinks> childLinks = unitLinksDAO.getUnitLinks(referencePeriod, childKey, childType);
                if (childLinks.isPresent()) {
                    // If the child unit also has children then fetch those
                    if (!childLinks.get().getChildren().isEmpty()) {
                        childUnit = createUnitHierachy(referencePeriod, childUnit, childLinks.get());
                    }
                }
                parentUnit.addChild(childUnit);
            }
        }
        return parentUnit;
    }

}
