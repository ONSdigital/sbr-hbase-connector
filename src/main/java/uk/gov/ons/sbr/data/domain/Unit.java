package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;

/**
 * Represents a definition of an statistical unit for a reference period i.e. a year and month
 * Each unit is of a defined type and may or may not contain parent and child links to other units
 *
 * @see UnitType
 */
public class Unit extends AbstractPeriodEntity {

    private UnitType type;
    private UnitLinks links;

    public Unit(YearMonth referencePeriod, String key, UnitType type) {
        super(referencePeriod, key);
        this.type = type;
        this.links = new UnitLinks(referencePeriod, key);
    }

    public UnitType getType() {
        return type;
    }

    public UnitLinks getLinks() {
        return links;
    }

    public void setLinks(UnitLinks links) {
        this.links = links;
    }
}
