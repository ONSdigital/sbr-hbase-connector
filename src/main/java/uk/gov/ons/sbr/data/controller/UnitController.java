package uk.gov.ons.sbr.data.controller;

import uk.gov.ons.sbr.data.dao.StatisticalUnitLinksDAO;
import uk.gov.ons.sbr.data.domain.StatisticalUnit;
import uk.gov.ons.sbr.data.domain.UnitLinks;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.dao.HBaseStatisticalUnitLinksDAO;
import uk.gov.ons.sbr.data.hbase.util.ReferencePeriodUtils;

import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Provides read and update access to the SBR Enterprise operational data
 */
public class UnitController {

    private StatisticalUnitLinksDAO unitLinksDAO;

    public UnitController() {
        this.unitLinksDAO = new HBaseStatisticalUnitLinksDAO();
    }

    public Optional<List<StatisticalUnit>> findUnits(String unitReferenceNumber) throws Exception {
        return findUnits(ReferencePeriodUtils.getCurrentPeriod(), unitReferenceNumber);
    }

    public Optional<List<StatisticalUnit>> findUnits(YearMonth referencePeriod, String unitReferenceNumber) throws Exception {
        return unitLinksDAO.scanUnits(referencePeriod, unitReferenceNumber);
    }

    public Optional<UnitLinks> getUnitLinks(YearMonth referencePeriod, String unitReferenceNumber, UnitType type) throws Exception {
        return unitLinksDAO.getUnitLinks(referencePeriod, unitReferenceNumber, type);
    }

    public void updateUnitLinks(YearMonth referencePeriod, String unitKey, UnitType type, Map<UnitType, String> parents, Map<String, UnitType> children) throws Exception {
        UnitLinks updatedLinks = new UnitLinks(referencePeriod, unitKey);
        if (parents != null) {
            updatedLinks.setParents(parents);
        }
        if (children != null) {
            updatedLinks.setChildren(children);
        }
        unitLinksDAO.putUnitLinks(updatedLinks, type);
    }

}
