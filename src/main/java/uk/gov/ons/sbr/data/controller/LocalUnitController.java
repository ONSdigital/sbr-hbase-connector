package uk.gov.ons.sbr.data.controller;

import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.dao.LocalUnitDAO;
import uk.gov.ons.sbr.data.dao.StatisticalUnitLinksDAO;
import uk.gov.ons.sbr.data.domain.*;
import uk.gov.ons.sbr.data.hbase.dao.HBaseEnterpriseDAO;
import uk.gov.ons.sbr.data.hbase.dao.HBaseLocalUnitDAO;
import uk.gov.ons.sbr.data.hbase.dao.HBaseStatisticalUnitLinksDAO;
import uk.gov.ons.sbr.data.hbase.util.ReferencePeriodUtils;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides read and update access to the SBR local unit operational data
 */
public class LocalUnitController {

    private LocalUnitDAO localUnitDAO;
    private StatisticalUnitLinksDAO unitLinksDAO;

    public LocalUnitController() {
        this.localUnitDAO = new HBaseLocalUnitDAO();
        this.unitLinksDAO = new HBaseStatisticalUnitLinksDAO();
    }

    public Optional<LocalUnit> getLocalUnit(String localUnitReferenceNumber) throws Exception {
        return getLocalUnitForReferencePeriod(ReferencePeriodUtils.getCurrentPeriod(), localUnitReferenceNumber);
    }

    public Optional<LocalUnit> getLocalUnitForReferencePeriod(YearMonth referencePeriod, String localUnitReferenceNumber) throws Exception {
        Optional<LocalUnit> localUnit = localUnitDAO.getLocalUnit(referencePeriod, localUnitReferenceNumber);
        if (localUnit.isPresent()) {
            Optional<UnitLinks> links = unitLinksDAO.getUnitLinks(referencePeriod, localUnitReferenceNumber, UnitType.LOCAL_UNIT);
            links.ifPresent(unitLinks -> localUnit.get().setLinks(unitLinks));
        }
        return localUnit;
    }

    public void updateLocalUnitVariableValue(YearMonth referencePeriod, String localUnitReferenceNumber, String variableName, String newValue) throws Exception {
        LocalUnit updatedLocalUnit = new LocalUnit(referencePeriod, localUnitReferenceNumber);
        updatedLocalUnit.putVariable(variableName, newValue);
        localUnitDAO.putLocalUnit(updatedLocalUnit);
    }

    public void updateLocalUnitVariableValues(YearMonth referencePeriod, String localUnitReferenceNumber, Map<String, String> newVariableValues) throws Exception {
        LocalUnit updatedLocalUnit = new LocalUnit(referencePeriod, localUnitReferenceNumber);
        updatedLocalUnit.putVariables(newVariableValues);
        localUnitDAO.putLocalUnit(updatedLocalUnit);
    }

}
