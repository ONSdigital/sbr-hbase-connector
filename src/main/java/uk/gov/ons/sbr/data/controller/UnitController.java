package uk.gov.ons.sbr.data.controller;

import uk.gov.ons.sbr.data.dao.UnitDAO;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.domain.Unit;
import uk.gov.ons.sbr.data.domain.UnitLinks;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.HBaseConfig;
import uk.gov.ons.sbr.data.hbase.dao.HBaseUnitDAO;

import java.io.IOException;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Provides read and update access to the SBR Enterprise operational data
 */
public class UnitController {

    private UnitDAO unitLinksDAO;

    public UnitController(HBaseConfig config) {
        this.unitLinksDAO = new HBaseUnitDAO(config);
    }

    public Optional<List<Unit>> findUnits(YearMonth referencePeriod, String unitReferenceNumber) throws IOException {
        return unitLinksDAO.scanUnits(referencePeriod, unitReferenceNumber);
    }

    void updateUnitLinks(YearMonth referencePeriod, String unitKey, UnitType type, Map<UnitType, String> parents, String childrenJsonAsString) throws IOException {
        UnitLinks updatedLinks = new UnitLinks(referencePeriod, unitKey);
        if (parents != null) {
            updatedLinks.setParents(parents);
        }
        if (childrenJsonAsString != null){
            updatedLinks.setChildJsonString(childrenJsonAsString);
        }
        unitLinksDAO.putUnitLinks(updatedLinks, type);
    }

}
